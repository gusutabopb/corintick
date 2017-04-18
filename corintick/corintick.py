"""
Functions for retrieving data from Corintick
"""
from typing import Iterable

import pandas as pd
import pymongo
from bson import SON, CodecOptions
from pymongo import IndexModel
from pymongo.results import InsertOneResult, BulkWriteResult

from . import serialization
from .utils import make_logger

logger = make_logger(__name__)
opts = CodecOptions(document_class=SON)
MAX_DOCS = 10


class Corintick:
    def __init__(self, client=None, dbname='corintick', bucket='corintick'):
        self.client = client or pymongo.MongoClient()
        self.db = self.client.get_database(dbname)
        self.bucket = self.db.get_collection(bucket).with_options(opts)
        self._make_indexes()
        self.MAX_DOCS = MAX_DOCS

    def _query(self, uid, start, end, columns, **metadata):
        # The following represent docs 1) containing query start,
        # OR 2) between query start and query end, OR 3) containing query end
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

        print(query, projection)
        cursor = self.bucket.find(query, projection).limit(self.MAX_DOCS).sort('start')

        return cursor

    def read(self, uid, start=pd.Timestamp.min, end=pd.Timestamp.max, columns=None, **metadata):
        cursor = self._query(uid, start, end, columns, **metadata)
        exec_stats = cursor.explain()
        ndocs = exec_stats['executionStats']['nReturned']

        if not ndocs:
            logger.warning('No documents retrieved!')
            df = None
        elif ndocs > self.MAX_DOCS:
            logger.warning(f'More than {self.MAX_DOCS} found. Returning only the '
                           f'first {self.MAX_DOCS} docs. Change `corintick.Reader.MAX_DOCS` property '
                           f'or change query to retrieve remaining docs.')
            df = serialization.build_dataframe(cursor)
        else:
            df = serialization.build_dataframe(cursor)

        if columns and ndocs:
            not_found = set(columns) - set(df.columns)
            if not_found:
                logger.warning(f'The following requested columns were not found: {not_found}')

        return df

    def list_uids(self):
        project = {'uid': 1, 'start': 1, 'end': 1, 'metadata': 1}
        group = {'_id': '$uid',
                 'doc_count': {'$sum': 1},
                 'start': {'$min': '$start'},
                 'end': {'$max': '$end'},
                 'total_rows': {'$sum': '$metadata.nrows'}}

        agg = self.bucket.aggregate([{"$project": project}, {"$group": group}])
        return list(agg)

    def list_metadata(self):
        pass

    def _make_indexes(self):
        """
        Makes indexes used by Corintick.
        Metadata is not used for querying and therefore not indexed.
        Making `ix1` unique is meant to avoid accidentaly inserting duplicate documents.
        """
        ix1 = IndexModel([('uid', 1), ('start', -1), ('end', -1)], unique=True, name='default')
        ix2 = IndexModel([('uid', 1), ('end', -1), ('start', -1)], name='reverse')
        self.bucket.create_indexes([ix1, ix2])

    def write(self, uid: str, df: pd.DataFrame, **metadata) -> InsertOneResult:
        """
        Writes a single timeseries DataFrame to Corintick.

        :param uid: String-like unique identifier for the timeseries
        :param df: DataFrame representing a timeseries segment
        :param metadata: Dictionary-like object containing metadata about
                         the underlying streamers, such as streamers source, etc.
        :return: None
        """
        doc = serialization.make_bson_doc(uid, df, **metadata)
        result = self.bucket.insert_one(doc)
        return result

    def bulk_write(self, it: Iterable) -> BulkWriteResult:
        """
        Takes an iterable which returns a (uid, df, metadata) tuple every iteration.
        See `corintick.Writer.write` docstring for tuple object types.
        This function can be used with simple lists or more complex generator objects.

        :param it: Iterator containing streamers to be inserted
        :return: None
        """
        bulk = self.bucket.initialize_ordered_bulk_op()
        for data in it:
            uid, df, metadata = data
            for sub_df in serialization.split_dataframes(df):
                doc = serialization.make_bson_doc(uid, sub_df, **metadata)
                bulk.insert(doc)
        result = bulk.execute()
        return result
