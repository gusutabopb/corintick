"""
Functions for retrieving data from Corintick
"""
from collections import OrderedDict
from typing import Iterable, Optional

import pandas as pd
import pymongo
from bson import CodecOptions
from pymongo import IndexModel
from pymongo.results import BulkWriteResult

from . import serialization
from . import utils


class Corintick:
    MAX_DOCS = 10

    def __init__(self, config):
        self.config = utils.load_config(config)
        self.logger = utils.make_logger('corintick', self.config)
        self.client = pymongo.MongoClient(**self.config['host'])
        if 'auth' in self.config:
            self.client.admin.authenticate(**self.config['auth'])
        self.db = self.client.get_database(**self.config['database'])
        self.opts = CodecOptions(document_class=OrderedDict, tz_aware=True)
        self.current_collection = self.db.get_collection(self.collections[0]).with_options(self.opts)
        for collection in self.collections:
            self._make_indexes(collection)

    @property
    def collections(self):
        return self.config['collections']

    @property
    def collection(self):
        return self.current_collection

    @collection.setter
    def collection(self, value):
        self.current_collection = self.get_collection(value)

    def get_collection(self, collection):
        if collection is None:
            return self.current_collection
        elif collection in self.collections:
            return self.db.get_collection(collection).with_options(self.opts)
        else:
            raise CorintickValidationError("Collection doesn't exist. Please add it to the config file.")

    def _query(self, uid, start, end, columns, collection, **metadata):
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
        col = self.get_collection(collection)
        cursor = col.find(query, projection).limit(self.MAX_DOCS).sort('start')
        return cursor

    def read(self, uid, start=pd.Timestamp.min, end=pd.Timestamp.max,
             columns=None, collection=None, **metadata) -> Optional[pd.DataFrame]:
        """
        Fetches data from Corintick's default collection.
        :param uid: Unique identifier of the timeseries
        :param start: ISO-8601 string or datetime-like object
        :param end: ISO-8601 string or datetime-like object
        :param columns: Columns to retrieve
        :param collection: Collection to be used (optional)
        :param metadata: MongoDB query dictionary
        """
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        cursor = self._query(uid, start, end, columns, collection, **metadata)
        exec_stats = cursor.explain()
        ndocs = exec_stats['executionStats']['nReturned']

        if not ndocs:
            self.logger.warning('No documents retrieved!')
            return
        elif ndocs > self.MAX_DOCS:
            self.logger.warning(f'More than {self.MAX_DOCS} found. Returning only the '
                                f'first {self.MAX_DOCS} docs. Change `corintick.Reader.MAX_DOCS` property '
                                f'or change query to retrieve remaining docs.')
        df = serialization.build_dataframe(cursor)

        if columns and ndocs:
            not_found = set(columns) - set(df.columns)
            if not_found:
                self.logger.warning(f'The following requested columns were not found: {not_found}')

        return df.ix[start:end]

    def list_uids(self, collection=None):
        """Returns list of UIDs contained in collection"""
        project = {'uid': 1, 'start': 1, 'end': 1, 'metadata': 1}
        group = {'_id': '$uid',
                 'doc_count': {'$sum': 1},
                 'start': {'$min': '$start'},
                 'end': {'$max': '$end'},
                 'total_rows': {'$sum': '$metadata.nrows'},
                 'total_size': {'$sum': '$metadata.binary_size'}}

        col = self.get_collection(collection)
        result = col.aggregate([{"$project": project}, {"$group": group}])
        return list(result)

    def list_metadata(self):
        """Returns document statistics grouped by metadata parameters"""
        pass

    def _make_indexes(self, collection):
        """
        Makes indexes used by Corintick.
        Metadata is not used for querying and therefore not indexed.
        Making `ix1` unique is meant to avoid accidentaly inserting duplicate documents.
        """
        ix1 = IndexModel([('uid', 1), ('start', -1), ('end', -1)], unique=True, name='default')
        ix2 = IndexModel([('uid', 1), ('end', -1), ('start', -1)], name='reverse')
        col = self.get_collection(collection)
        col.create_indexes([ix1, ix2])

    def _validate_dates(self, uid, df, collection):
        """Checks whether new DataFrame has date conflicts with existing documents"""
        if not df.index.tzinfo:
            raise ValueError('DatetimeIndex must be timezone-aware')
        col = self.get_collection(collection)
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

    def write(self, uid: str, df: pd.DataFrame, collection: Optional[str]=None, **metadata):
        """
        Writes a single timeseries DataFrame to Corintick.

        :param uid: String-like unique identifier for the timeseries
        :param df: DataFrame representing a timeseries segment
        :param collection: Collection to be used (optional)
        :param metadata: Dictionary-like object containing metadata about
                         the underlying streamers, such as streamers source, etc.
        :return: None
        """
        self._validate_dates(uid, df, collection)
        return self.bulk_write([(uid, df, metadata)])

    def bulk_write(self, it: Iterable, collection: Optional[str]=None) -> BulkWriteResult:
        """
        Takes an iterable which returns a tuple containing the arguments to
        `Corintick.write` tuple for every iteration.
        This function can be used with simple lists or more complex generator objects.

        :param it: Iterator containing streamers to be inserted
        """
        bulk = self.current_collection.initialize_ordered_bulk_op()
        for data in it:
            uid, df, metadata = data
            self._validate_dates(uid, df, collection)
            docs = serialization.make_bson_docs(uid, df, metadata)
            for doc in docs:
                bulk.insert(doc)
        result = bulk.execute()
        return result


class CorintickValidationError(ValueError):
    pass