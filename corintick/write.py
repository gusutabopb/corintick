"""
Functions for writing data to Corintick
"""

import pymongo

import pandas as pd
from bson import SON, CodecOptions

from .serialization import make_bson_doc
from .utils import make_logger

logger = make_logger(__name__)

class Writer:

    def __init__(self, client=None, dbname='corintick', bucket='corintick'):
        self.client = client or pymongo.MongoClient()
        self.db = self.client.get_database(dbname)
        self.bucket = self.db.get_collection(bucket)
        self.meta = self.db.get_collection(f'{bucket}.meta')

    def write(self, uid, df, **metadata):
        doc = make_bson_doc(uid, df, **metadata)
        self.bucket.insert_one(doc)

