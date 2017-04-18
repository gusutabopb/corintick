import pandas as pd
import pytest

from corintick import Corintick

@pytest.fixture(scope="module")
def api():
    api = Corintick(config='~/plugaai/corintick/config.yml')
    print(api.config)
    return api


def test_simple_write(api):
    import pandas_datareader as pdr
    import quandl
    uid1 = 'AAPL'
    uid2 = '7203'
    df1 = pdr.get_data_yahoo(uid1)
    df2 = quandl.get('TSE/{}'.format(uid2))
    print(df1.head())
    print(df2.head())
    r1 = api.write(uid1, df1, source='Yahoo Finance')
    r2 = api.write(uid2, df2, source='Quandl')
    assert r1.acknowledged
    assert r2.acknowledged


def test_simple_read(api):
    for df in [api.read('AAPL'), api.read('7203')]:
        assert isinstance(df, pd.DataFrame)
        print(df.head(), df.shape)
        assert len(df) > 100

