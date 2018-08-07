import datetime
import logging

import pandas as pd
import numpy as np
import pytest

from corintick import Corintick

logger = logging.getLogger('corintick')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s | %(name)s | %(levelname)s: %(message)s'))
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def api():
    api = Corintick(db='corintick_test')
    yield api
    assert 'corintick_test' in api.client.database_names()
    api.client.drop_database('corintick_test')
    assert 'corintick_test' not in api.client.database_names()


def make_ohlc(freq='1D'):
    start = 1388534400000000000  # 2014-01-01
    N = 500000
    ix = start + np.random.randint(10 ** 9, 5 * 10 ** 11, size=N).cumsum()
    df = pd.DataFrame(
        index=pd.to_datetime(ix, utc=True),
        data={'price': np.random.randn(N).cumsum(axis=0)}
    )
    df = df.resample(freq).agg({'price': 'ohlc'}).round(2)
    return (df - df.min().min() + 10)['price']


def test_write_ohlc(api):
    df = make_ohlc()
    print(df.head())
    r = api.write('OHLC', df[:100], freq='1D')
    assert r.acknowledged
    _ = api.write('OHLC', df[100:], freq='1D')
    assert api.db.corintick.count() == 2


def test_read_ohlc(api):
    df = api.read('OHLC', columns=['high', 'low'])
    print(df.head())
    nrows, ncols = df.shape
    assert nrows > 100
    assert ncols == 2


def test_date_parsing(api):
    df = api.read('OHLC', start='2015-01-01', end='2015-12-31')
    start = df.index[0].date()
    end = df.index[-1].date()
    assert start == datetime.date(2015, 1, 1)
    assert end == datetime.date(2015, 12, 31)
    assert len(df) == 365


def test_date_validation(api):
    df = api.read('OHLC', start='2015', end='2016')
    with pytest.raises(ValueError):
        api.write('OHLC', df)


def test_write_random_walk(api):
    N = 500000
    ix = pd.Timestamp.now().value + np.random.randint(10 ** 9, 10 ** 11, size=N).cumsum()
    df = pd.DataFrame(
        index=pd.to_datetime(ix, utc=True),
        data=np.random.randn(N, 5).cumsum(axis=0)
    )
    df['string'] = 'string'
    result = api.write('RDWK', df)
    assert len(result.inserted_ids) == 2
    df = api.read('RDWK')
    assert len(df) == N


def test_list_uid(api):
    df = pd.DataFrame(api.list_uids())
    print(df)
    assert len(df) == 2


def test_inexistent_uid(api):
    assert api.read('FOO') is None


def test_missing_columns(api):
    with pytest.warns(UserWarning):
        api.read('OHLC', columns=['close', 'volume'])


def test_max_docs_warning(api):
    with pytest.warns(UserWarning):
        _ = api.read('RDWK', max_docs=1)


def test_tznaive_warning(api):
    df = make_ohlc(freq='1h')
    df.index = df.index.tz_convert(None)
    with pytest.warns(UserWarning):
        api.write('TZ', df)


def test_non_datetimeindex(api):
    df = make_ohlc().reset_index()
    with pytest.raises(ValueError):
        api.write('DTIX', df)


def test_different_collection(api):
    api.write('SYM', make_ohlc(), collection='mycollection')
    assert 'mycollection' in api.collections
