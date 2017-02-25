"""
Sample usage
"""

import pymongo

from corintick.compression import make_bson
from corintick.parsers import BFSnapshotParser

db = pymongo.MongoClient('localhost', 27017, connect=False).test

if __name__ == '__main__':
    df = BFSnapshotParser().data
    doc = make_bson()
    db.foo.insert_one(doc)
