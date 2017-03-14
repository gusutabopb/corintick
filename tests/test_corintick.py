import pandas as pd
import pytest

from corintick import Writer, Reader

@pytest.fixture
def writer():
    return Writer(dbname='corintick_test')


@pytest.fixture
def reader():
    return Reader(dbname='corintick_test')


def test_simple_write(writer):
    import pandas_datareader as pdr
    import quandl
    uid1 = 'AAPL'
    uid2 = '7203'
    df1 = pdr.get_data_yahoo(uid1)
    df2 = quandl.get('TSE/{}'.format(uid2))
    r1 = writer.write(uid1, df1, source='Yahoo Finance')
    r2 = writer.write(uid2, df2, source='Quandl')
    assert r1.acknowledged
    assert r2.acknowledged


def test_simple_read(reader):
    yahoo = reader.read('AAPL')
    quandl = reader.read('7203')
    assert len(yahoo) > 100
    assert len(quandl) > 100
    assert isinstance(yahoo, pd.DataFrame)
    assert isinstance(yahoo, pd.DataFrame)
