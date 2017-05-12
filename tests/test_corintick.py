import pandas as pd
import numpy as np
import pytest

from corintick import Corintick

@pytest.fixture(scope="module")
def api():
    api = Corintick(config='unittest_config.yml')
    print(api.config)
    yield api
    assert 'corintick_test' in api.client.database_names()
    api.client.drop_database('corintick_test')
    assert 'corintick_test' not in api.client.database_names()


def test_simple_write(api):
    import pandas_datareader as pdr
    import quandl
    uid1 = 'AAPL'
    uid2 = '7203'
    df1 = pdr.get_data_yahoo(uid1)
    df2 = quandl.get(f'TSE/{uid2}')
    print(df1.head())
    print(df2.head())
    r1 = api.write(uid1, df1, source='Yahoo Finance')
    r2 = api.write(uid2, df2, source='Quandl')
    assert r1['nInserted']
    assert r2['nInserted']
    assert api.db.corintick.count() == 2


def test_simple_read(api):
    for df in [api.read('AAPL'), api.read('7203')]:
        assert isinstance(df, pd.DataFrame)
        print(df.head(), df.shape)
        assert len(df) > 100


def test_write_low_compression_data(api):
    ix = pd.date_range('2007-01-01', '2017-01-01').to_series()
    ix = ix.groupby(pd.TimeGrouper('40min')).count()
    df = pd.DataFrame(index=ix.index[:10 ** 5],
                      data={i: np.arange(10 ** 5) for i in range(100)})
    result = api.write('LCDF', df)
    assert result['nInserted'] == 3
    df = api.read('LCDF')
    assert len(df) == 100000


def test_list_uid(api):
    df = pd.DataFrame(api.list_uids())
    print(df)
    assert len(df) == 3