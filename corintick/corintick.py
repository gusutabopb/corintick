"""
Functions for retrieving data from Corintick
"""
import logging
import warnings
from typing import Optional, Sequence, Mapping

import pandas as pd
import pymongo
from bson import CodecOptions
from pymongo import IndexModel
from pymongo.results import InsertManyResult

from . import serialization


class Corintick:
    def __init__(
            self,
            host='localhost',
            port=27017,
            db=None,
            collection='corintick',
            username=None,
            password=None,
    ):
        self.logger = logging.getLogger('corintick')
        self.client = pymongo.MongoClient(host=host, port=port)
        if username and password:
            self.client.admin.authenticate(name=username, password=password)
        self.db = self.client.get_database(db or 'corintick')
        self.default_collection = collection
        self.max_docs = 20
        for collection in self.collections:
            self._make_indexes(collection)

    @property
    def collections(self):
        return self.db.collection_names()

    def read(
            self,
            uid,
            start=pd.Timestamp.min,
            end=pd.Timestamp.max,
            columns=None,
            collection=None,
            max_docs=None,
            **metadata
    ) -> Optional[pd.DataFrame]:
        """Fetches data from Corintick's default collection.

        :param uid: Unique identifier of the timeseries
        :param start: ISO-8601 string or datetime-like object
        :param end: ISO-8601 string or datetime-like object
        :param columns: Columns to retrieve
        :param collection: Collection to be used (optional)
        :param metadata: MongoDB query dictionary
        :param max_docs: Limit number of documents to be retrieved
            from MongoDB per query (defaults to self.max_docs)
        """
        max_docs = max_docs or self.max_docs
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        cursor = self._query(uid, start, end, columns, collection, max_docs, **metadata)
        exec_stats = cursor.explain()
        ndocs = exec_stats['executionStats']['nReturned']

        if not ndocs:
            self.logger.warning('No documents retrieved!')
            return

        df = serialization.build_dataframe(cursor)
        if max_docs and ndocs >= max_docs:
            doc_stats = self.list_uids(uid=uid, collection=collection)[0]
            warnings.warn(
                f"Only {ndocs} docs retrieved. There are {doc_stats['doc_count']} docs for {uid}, "
                f"ranging from {doc_stats['start'].date()} to {doc_stats['end'].date()}. "
                f"Increase `max_docs` or change date range to retrieve more data. "
                f"Setting `max_docs` to 0 will fetch an unlimited number of documents."
            )

        if columns and ndocs:
            not_found = set(columns) - set(df.columns)
            if not_found:
                warnings.warn(f'The following requested columns were not found: {not_found}')

        return df.loc[start:end]

    def write(
            self,
            uid: str,
            df: pd.DataFrame,
            collection: Optional[str] = None,
            **metadata
    ) -> InsertManyResult:
        """Writes a timeseries DataFrame to Corintick.

        :param uid: Unique identifier for the timeseries
        :param df: DataFrame representing a timeseries segment
        :param collection: Collection to be used (optional)
        :param metadata: Timeseries metadata such as exchange, source, etc.
        """
        self._validate_dates(uid, df, collection)
        docs = serialization.make_bson_docs(uid, df, metadata)
        result = self._get_collection(collection).insert_many(docs)
        return result

    def list_uids(
            self,
            uid: Optional[str] = None,
            collection: Optional[str] = None
    ) -> Sequence[Mapping]:
        """Returns list of UIDs contained in collection

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
        return list(col.aggregate(pipeline))

    def list_metadata(self):
        """Returns document statistics grouped by metadata parameters"""
        # TODO: Implement
        raise NotImplementedError()

    def _query(self, uid, start, end, columns, collection,
               max_docs, **metadata) -> pymongo.cursor.Cursor:
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
            projection.update({f'columns.{col}': 1 for col in columns})

        self.logger.debug(query, projection)
        col = self._get_collection(collection)
        return col.find(query, projection).limit(max_docs)

    def _make_indexes(self, collection: str) -> None:
        """Makes indexes used by Corintick.
        Metadata is not used for querying and therefore not indexed.
        Making `ix1` unique is meant to avoid accidentally inserting duplicate documents.
        """
        ix1 = IndexModel([('uid', 1), ('start', -1), ('end', -1)], unique=True, name='default')
        ix2 = IndexModel([('uid', 1), ('end', -1), ('start', -1)], name='reverse')
        self.db.get_collection(collection).create_indexes([ix1, ix2])

    def _validate_dates(
            self,
            uid: str,
            df: pd.DataFrame,
            collection: str
    ) -> None:
        """Checks whether new DataFrame has date conflicts with existing documents"""
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError('DataFrame index is not DatetimeIndex')
        tz_aware = True if df.index.tzinfo else False
        col = self._get_collection(collection, tz_aware=tz_aware)
        docs = col.find({'uid': uid}, {'uid': 1, 'start': 1, 'end': 1})
        df = df.sort_index()
        start = df.index[0]
        end = df.index[-1]
        for d in sorted(docs, key=lambda x: x['start']):
            if end < d['start'] or start > d['end']:
                continue
            raise ValueError(
                f"Invalid dates ({start} ~ {end}). "
                f"Conflicts with document {d['_id']} ({d['uid']}: {d['start']}~{d['end']})"
            )

    def _get_collection(
            self,
            collection: Optional[str] = None,
            tz_aware=False,
    ) -> pymongo.collection.Collection:
        """Parses codec options and returns MongoDB collection objection"""
        if collection is None:
            collection = self.default_collection
        elif collection not in self.collections:
            self._make_indexes(collection)
            self.logger.info(f'Making new collection: {collection}')
        opts = CodecOptions(tz_aware=tz_aware)
        return self.db.get_collection(collection).with_options(opts)
