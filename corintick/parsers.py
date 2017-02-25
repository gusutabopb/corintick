"""
Insert stuff that gets that from a source and returns a
 ready-to compress DataFrame. This functionality can (should?)
 be implemented on the data source side (repo).
"""
import pymongo
import itertools

import pandas as pd


class BFSnapshotParser:
    cols = [('L{}AskPrice'.format(i + 1), 'L{}AskSize'.format(i + 1)) for i in range(300)]
    cols = list(itertools.chain(*cols))

    def __init__(self):
        db = pymongo.MongoClient('localhost', 27025, connect=False).btc
        snaps = db['lightning_board_snapshot_BTC_JPY'].find()
        self._data = pd.concat([self.make_row(snap) for snap in snaps], axis=1).T
        self._data.columns = self.cols

    @property
    def data(self):
        return self._data

    @staticmethod
    def make_row(snap, side='asks'):
        df = pd.DataFrame.from_records(snap[side])
        price = df['price'].astype('int').copy()
        size = df['size'].copy()
        size.index = pd.Index([i + 0.5 for i in df[['size']].index])
        row = pd.concat([price, size], axis=0).sort_index()
        row.name = pd.Timestamp(snap['timetoken'])
        return row
