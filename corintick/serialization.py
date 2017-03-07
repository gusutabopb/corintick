"""
Contains all the serialization/compression related functions
"""
import warnings
import lz4
import hashlib
from typing import Iterable
from io import BytesIO
from collections import OrderedDict

from bson import Binary, SON
import numpy as np
import pandas as pd

from .utils import make_logger

logger = make_logger(__name__)


def _serialize_array(arr: np.ndarray) -> bytes:
    """
    Serializes array using Numpy's native serialization functionality and
    compresses utilizing lz4's high compression algorithm.
    Arrays are serialized to C format and should be relatively easily to reverse
    engineer to other languages.
    Reference: https://docs.scipy.org/doc/numpy/neps/npy-format.html
    :param arr: Numpy array
    :return: Compressed bytes
    """
    if arr.dtype == np.dtype('O'):
        logger.warning('Attemping to serialize a Python object')
    with BytesIO() as f:
        np.save(f, arr)
        f.seek(0)
        output = f.read()
    return lz4.compressHC(output)


def _deserialize_array(data: bytes) -> np.ndarray:
    """
    Takes raw binary compressesed/serialized retrieved from MongoDB
    and decompresses/deserializes it, returning the original Numpy array
    :param data: Input binary data
    :return: Numpy array
    """
    return np.load(BytesIO(lz4.decompress(data)))


def _make_bson_column(col: pd.Series) -> dict:
    """
    Compresses dataframe's column/index and returns a dictionary
    with BSON blob column data and some metadata.
    :param arr: Input column data
    :return: Column data dictionary
    """
    data = Binary(_serialize_array(col.values))
    sha1 = Binary(hashlib.sha1(data).digest())
    dtype = str(col.dtype)
    size = len(data)
    return {'data': data, 'dtype': dtype, 'sha1': sha1, 'size': size}


def make_bson_doc(uid: str, df: pd.DataFrame, **metadata) -> SON:
    """
    Takes a DataFrame and makes a BSON document ready to be inserted
    into MongoDB. Given Conritick's focus on timeseries data, the input
    DataFrame is assumed to have a sorted DatetimeIndex and non-compling
    data however that is not strictly mandatory. Column name data is kept, but index
    name data is explicitly discarded and not saved.

    The output BSON document can't be larger than 16 MB, so the input DataFrame
    should take that into consideration. Use `pandas.DataFrame.memory_usage().sum()`
    to see total memory consumption of input. A maximum memory usage of 64MB should
    be fine, but your milege may vary depending on compressibility of input.

    :param df: Input DataFrame
    :param metadata: Any BSON-able objects to be attached to document as metadata
    :return: BSON document
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        msg = 'DataFrame index is not DatetimeIndex'
        warnings.warn(msg)
        logger.debug(msg)

    index = _make_bson_column(df.index)
    columns = SON()
    for col in df.columns:
        columns[col] = _make_bson_column(df[col])

    nrows = len(df)
    col_data_size = sum([columns[col]['size'] for col in df.columns])
    logger.info(f'Column data size: {col_data_size:,} bytes / {nrows}')
    metadata.update({'nrows': nrows, 'binary_size': col_data_size})

    doc = SON([
        ('uid', uid),
        ('start', df.index[0]),
        ('end', df.index[-1]),
        ('metadata', metadata),
        ('index', index),
        ('columns', columns)])

    return doc


def _build_dataframe(doc: SON) -> pd.DataFrame:
    """
    Builds DataFrame from passed BSON document. Input BSON document must
    match schema defined at `make_bson_doc`.
    :param doc: BSON document
    :return: DataFrame
    """
    index = pd.Index(_deserialize_array(doc['index']['data']))
    columns = [_deserialize_array(col['data']) for col in doc['columns'].values()]
    names = doc['columns'].keys()
    df = pd.DataFrame(index=index, data=OrderedDict(zip(names, columns)))
    return df


def build_dataframe(docs: Iterable[SON]) -> pd.DataFrame:
    """
    Concatenates multiple documents corresponding to the same
    :param docs:
    :return:
    """
    df: pd.DataFrame = pd.concat([_build_dataframe(doc) for doc in docs])
    return df.sort_index()