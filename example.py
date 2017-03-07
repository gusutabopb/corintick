"""
Sample usage
"""

import pymongo

from corintick.serialization import make_bson_doc
from corintick.parsers import BFSnapshotParser

db = pymongo.MongoClient('localhost', 27017, connect=False).test

if __name__ == '__main__':
    df = BFSnapshotParser().data
    doc = make_bson_doc(df)
    db.foo.insert_one(doc)
