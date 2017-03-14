"""
Functions for writing data to Corintick
"""

import pymongo

from pymongo import IndexModel
import pandas as pd
from bson import SON, CodecOptions

from .serialization import make_bson_doc, split_dataframes
from .utils import make_logger

logger = make_logger(__name__)

class Writer:

    def __init__(self, client=None, dbname='corintick', bucket='corintick'):
        self.client = client or pymongo.MongoClient()
        self.db = self.client.get_database(dbname)
        self.bucket = self.db.get_collection(bucket)
        self._make_indexes()

    def _make_indexes(self):
        """
        Makes indexes used by Corintick.
        Metadata is not used for querying and therefore not indexed.
        Making `ix1` unique is meant to avoid accidentaly inserting duplicate documents.
        """
        ix1 = IndexModel([('uid', 1), ('start', -1), ('end', -1)], unique=True, name='default')
        ix2 = IndexModel([('uid', 1), ('end', -1), ('start', -1)], name='reverse')
        self.bucket.create_indexes([ix1, ix2])

    def write(self, uid, df, **metadata):
        doc = make_bson_doc(uid, df, **metadata)
        self.bucket.insert_one(doc)
    def bulk_write(self, it: Iterable) -> BulkWriteResult:
        """
        Takes an iterable which returns a (uid, df, metadata) tuple every iteration.
        See `corintick.Writer.write` docstring for tuple object types.
        This function can be used with simple lists or more complex generator objects.

        :param it: Iterator containing data to be inserted
        :return: None
        """
        bulk = self.bucket.initialize_ordered_bulk_op()
        for data in it:
            uid, df, metadata = data
            for sub_df in split_dataframes(df):
                doc = make_bson_doc(uid, sub_df, **metadata)
                bulk.insert(doc)
        result = bulk.execute()
        return result

