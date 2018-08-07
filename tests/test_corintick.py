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
    print(api.config)
    yield api
    assert 'corintick_test' in api.client.database_names()
    api.client.drop_database('corintick_test')
    assert 'corintick_test' not in api.client.database_names()


def test_simple_write(api):
    import quandl
    uid = '7203'
    df = quandl.get(f'TSE/{uid}')
    print(df.head())
    r = api.write(uid, df, source='Quandl')
    assert r.acknowledged
    assert api.db.corintick.count() == 1


def test_simple_read(api):
    df = api.read('7203')
    assert isinstance(df, pd.DataFrame)
    print(df.head(), df.shape)
    assert len(df) > 100


def test_date_parsing(api):
    df = api.read('7203', start='2015', end='2016')
    start = df.index[0].date()
    end = df.index[-1].date()
    assert start == datetime.date(2015, 1, 5)
    assert end == datetime.date(2015, 12, 30)
    assert len(df) == 244


def test_date_validation(api):
    df = api.read('7203', start='2015', end='2016')
    with pytest.raises(ValueError):
        api.write('7203', df)


def test_write_random_walk(api):
    now = pd.Timestamp.now().value
    N = 500000
    df = pd.DataFrame(
        index=pd.to_datetime(now + np.random.randint(10 ** 9, 10 ** 11, size=N).cumsum(), utc=True),
        data=np.random.randn(N, 10).cumsum(axis=0)
    )
    result = api.write('RDWK', df)
    assert len(result.inserted_ids) == 3
    df = api.read('RDWK')
    assert len(df) == N


def test_list_uid(api):
    df = pd.DataFrame(api.list_uids())
    print(df)
    assert len(df) == 2
