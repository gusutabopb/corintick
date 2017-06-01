"""
Functions for retrieving data from Corintick
"""
from collections import OrderedDict
from typing import Optional, Sequence, Mapping

import pandas as pd
import pymongo
from bson import CodecOptions
from pymongo import IndexModel
from pymongo.results import InsertManyResult

from . import serialization
from . import utils

MIN_TIME = pd.Timestamp.min + pd.Timedelta(hours=48)
MAX_TIME = pd.Timestamp.max - pd.Timedelta(hours=48)


class Corintick:
    def __init__(self, config):
        self.config = utils.load_config(config)
        self.logger = utils.make_logger('corintick', self.config)
        self.client = pymongo.MongoClient(**self.config['host'])
        if 'auth' in self.config:
            self.client.admin.authenticate(**self.config['auth'])
        self.db = self.client.get_database(**self.config['database'])
        self.default_collection = self.config['collections'][0]
        self.default_codec_opts = dict(document_class=OrderedDict, tz_aware=True)
        for collection in self.config['collections']:
            self._make_indexes(collection)

    @property
    def collections(self):
        return self.db.collection_names()

    def read(self, uid, start=MIN_TIME, end=MAX_TIME,
             columns=None, collection=None, max_docs=20, **metadata) -> Optional[pd.DataFrame]:
        """
        Fetches data from Corintick's default collection.
        :param uid: Unique identifier of the timeseries
        :param start: ISO-8601 string or datetime-like object
        :param end: ISO-8601 string or datetime-like object
        :param columns: Columns to retrieve
        :param collection: Collection to be used (optional)
        :param metadata: MongoDB query dictionary
        :param max_docs: Limit number of documents to be retrieved from MongoDB per query
        """
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        cursor = self._query(uid, start, end, columns, collection, max_docs, **metadata)
        exec_stats = cursor.explain()
        ndocs = exec_stats['executionStats']['nReturned']

        if not ndocs:
            self.logger.warning('No documents retrieved!')
            return

        df = serialization.build_dataframe(cursor)
        if ndocs >= max_docs:
            doc_stats = self.list_uids(uid=uid)[0]
            params = (ndocs, doc_stats['doc_count'], uid,
                      doc_stats['start'].date(), doc_stats['end'].date())
            msg = ('Only {} docs retrieved. There are {} docs for {}, ranging from {} to {}. '
                   'Increase `max_docs` or change date range to retrieve more data.'.format(*params))
            self.logger.warning(msg)

        if columns and ndocs:
            not_found = set(columns) - set(df.columns)
            if not_found:
                self.logger.warning(f'The following requested columns were not found: {not_found}')

        return df.loc[start:end]

    def write(self, uid: str, df: pd.DataFrame, collection: Optional[str] = None, **metadata) -> InsertManyResult:
        """
        Writes a timeseries DataFrame to Corintick.
        :param uid: Unique identifier for the timeseries
        :param df: DataFrame representing a timeseries segment
        :param collection: Collection to be used (optional)
        :param metadata: Timeseries metadata such as exchange, source, etc.
        """
        self._validate_dates(uid, df, collection)
        docs = serialization.make_bson_docs(uid, df, metadata)
        result = self._get_collection(collection).insert_many(docs)
        return result

    def list_uids(self, uid: Optional[str] = None, collection: Optional[str] = None) -> Sequence[Mapping]:
        """
        Returns list of UIDs contained in collection
        :param uid: String-like unique identifier for the timeseries
        :param collection: Collection to be used (optional)
        """
        project = {'uid': 1, 'start': 1, 'end': 1, 'metadata': 1}
        group = {'_id': '$uid',
                 'doc_count': {'$sum': 1},
                 'start': {'$min': '$start'},
                 'end': {'$max': '$end'},
                 'total_rows': {'$sum': '$metadata.nrows'},
                 'total_size': {'$sum': '$metadata.binary_size'}}

        col = self._get_collection(collection)
        pipeline = [{"$project": project}, {"$group": group}]
        if uid:
            pipeline = [{'$match': {'uid': uid}}] + pipeline
        result = list(col.aggregate(pipeline))
        return result

    def list_metadata(self):
        """Returns document statistics grouped by metadata parameters"""
        # TODO: Implement
        raise NotImplementedError

    def _query(self, uid, start, end, columns, collection, max_docs, **metadata) -> pymongo.cursor.Cursor:
        # The following represent docs 1) containing query start,
        # OR 2) between query start and query end, OR 3) containing query end
        # TODO: Query multiple IDs by regex and/or metadata
        query = {'uid': uid}
        query['$or'] = [{'start': {'$lte': start}, 'end': {'$gte': start}},
                        {'start': {'$gte': start}, 'end': {'$lte': end}},
                        {'start': {'$lte': end}, 'end': {'$gte': end}}]
        for key, value in metadata.items():
            query[key] = value

        projection = {'uid': 1, 'start': 1, 'end': 1, 'metadata': 1, 'index': 1}
        if columns is None:
            projection.update({'columns': 1})
        else:
            projection.update({'columns.{}'.format(col): 1 for col in columns})

        self.logger.debug(query, projection)
        col = self._get_collection(collection)
        cursor = col.find(query, projection).limit(max_docs)
        return cursor

    def _make_indexes(self, collection: str) -> None:
        """
        Makes indexes used by Corintick.
        Metadata is not used for querying and therefore not indexed.
        Making `ix1` unique is meant to avoid accidentaly inserting duplicate documents.
        """
        ix1 = IndexModel([('uid', 1), ('start', -1), ('end', -1)], unique=True, name='default')
        ix2 = IndexModel([('uid', 1), ('end', -1), ('start', -1)], name='reverse')
        col = self._get_collection(collection)
        col.create_indexes([ix1, ix2])

    def _validate_dates(self, uid: str, df: pd.DataFrame, collection: str) -> None:
        """Checks whether new DataFrame has date conflicts with existing documents"""
        tz_aware = True if df.index.tzinfo else False
        if not tz_aware:
            self.logger.warning('DatetimeIndex is timezone-naive.')
        col = self._get_collection(collection, tz_aware=tz_aware)
        docs = col.find({'uid': uid}, {'uid': 1, 'start': 1, 'end': 1})
        df = df.sort_index()
        start = df.index[0]
        end = df.index[-1]
        for doc in sorted(docs, key=lambda x: x['start']):
            if end < doc['start'] or start > doc['end']:
                continue
            else:
                msg = 'Invalid dates. Conflicts with {} ({}: {}~{}) | Dataframe {}~{}'
                msg = msg.format(doc['_id'], doc['uid'], doc['start'], doc['end'], start, end)
                raise ValidationError(msg)

    def _get_collection(self, collection: Optional[str] = None, **options) -> pymongo.collection.Collection:
        """Parses codec options and returns MongoDB collection objection"""
        opts = {**self.default_codec_opts, **options}
        if 'tzinfo' in opts and not opts['tz_aware']:
            _ = opts.pop('tzinfo')
        opts = CodecOptions(**opts)

        if collection is None:
            collection = self.default_collection
        elif collection not in self.config['collections']:
            self._make_indexes(collection)
            self.logger.info(f'Making new collection: {collection}')
        return self.db.get_collection(collection).with_options(opts)


class ValidationError(ValueError):
    pass
