"""
Insert stuff that gets that from a source and returns a
 ready-to compress DataFrame. This functionality can (should?)
 be implemented on the data source side (repo).
"""
import pymongo
import re
import itertools

import pandas as pd
import numpy as np


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

class TRTHParser:

    def __init__(self):
        pass

    @staticmethod
    def pre_process(df, gtype) -> pd.DataFrame:
        """
        Loads TRTH CSV file and does pre-processing
        """
        df = df.dropna(axis=1, how='all').drop('Type', axis=1)
        # Removing chars which cause problems in MongoDB/Pandas (itertuples)
        df.columns = [re.sub('\.|-|#', '', col) for col in df.columns]
        if gtype == 'eod':
            df['Date[L]'] = pd.to_datetime(df['Date[L]'].astype(str))
            df.set_index('Date[L]', inplace=True)
            return df

        if 'Date[G]' in df.columns:
            date_col = 'Date[G]'
            time_col = 'Time[G]'
            dt_col = 'DateTime[G]'
        else:
            date_col = 'Date[L]'
            time_col = 'Time[L]'
            dt_col = 'DateTime[L]'
        df.index = pd.to_datetime(df[date_col].astype(str) + ' ' + df[time_col])
        df.index.name = dt_col
        df = df.drop([date_col, time_col], axis=1)
        if 'Qualifiers' in df.columns:
            df['Qualifiers'] = df['Qualifiers'].fillna('')

        drop_cols = ['Date[L]', 'Time[L]']
        df = df.drop(drop_cols, axis=1, errors='ignore')  # Silently ignores drop errors
        return df

    @staticmethod
    def fix_timestamps(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds 10 nanoseconds to repeated timestamps in order to make
        timeseries index unique.
        :param df: DataFrame
        :return: DataFrame
        """
        offset = pd.DataFrame(df.index).groupby('DateTime[L]').cumcount()
        offset = offset * np.timedelta64(10, 'ns')
        df.index = df.index.values + offset.values
        return df