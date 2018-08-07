"""
Contains all the serialization/compression related functions
"""
import datetime
import hashlib
import io
import logging
import re
import warnings
from typing import Iterable, List, Union, Mapping

import lz4.block
import numpy as np
import pandas as pd
import msgpack
import pytz
from bson import Binary, SON, InvalidBSON

logger = logging.getLogger('corintick')
MAX_BSON_SIZE = 2 ** 24  # 16 MB


def _serialize_array(arr: np.ndarray) -> bytes:
    """
    Serializes array using Numpy's native serialization functionality and
    compresses utilizing lz4's high compression algorithm.
    Numeric types are serialized to C format and should be relatively easily to reverse
    engineer to other languages (https://docs.scipy.org/doc/numpy/neps/npy-format.html).
    Non-numeric types are strigified and serialized to MessagePack (http://msgpack.org/index.html).
    :param arr: Numpy array
    :return: LZ4-compressed binary blob
    """
    if arr.dtype == np.dtype('O'):
        data = msgpack.dumps(list(map(str, arr)))
    else:
        with io.BytesIO() as f:
            np.save(f, arr)
            f.seek(0)
            data = f.read()
    blob = lz4.block.compress(data, mode='high_compression')
    return blob


def _deserialize_array(column: Mapping) -> np.ndarray:
    """
    Takes raw binary compressed/serialized retrieved from MongoDB
    and decompresses/deserializes it, returning the original Numpy array
    :param column: Input column
    :return: Numpy array
    """
    data = lz4.block.decompress(column['blob'])
    if column['dtype'] == 'object':
        return np.array([i.decode('utf-8') for i in msgpack.loads(data)])
    else:
        return np.load(io.BytesIO(data))


def _make_bson_column(col: Union[pd.Series, pd.DatetimeIndex]) -> SON:
    """
    Compresses dataframe's column/index and returns a dictionary
    with BSON blob column and some metadata.

    :param col: Input column/index
    :return: Column data dictionary
    """
    blob = Binary(_serialize_array(col.values))
    sha1 = Binary(hashlib.sha1(blob).digest())
    dtype = str(col.dtype)
    size = len(blob)
    return SON(blob=blob, dtype=dtype, sha1=sha1, size=size)


def _make_bson_doc(uid: str, df: pd.DataFrame, metadata) -> SON:
    """
    Takes a DataFrame and makes a BSON document ready to be inserted
    into MongoDB. Given Conritick's focus on timeseries data, the input
    DataFrame index must be a DatetimeIndex.
    Column names are kept and saved as strings.
    Index name is explicitly discarded and not saved.

    :param uid: Unique ID for the timeseries represented by the input DataFrame
    :param df: Input DataFrame
    :param metadata: Any BSON-able objects to be attached to document as metadata
    :return: BSON document
    """
    mem_usage = df.memory_usage().sum()
    df = df.sort_index(ascending=True)

    if df.index.tzinfo is None:
        if not all(ix.time() == datetime.time(0, 0) for ix in df.index[:100]):
            # Issue warning only if DataFrame doesn't look like EOD based.
            warnings.warn('DatetimeIndex is timezone-naive. Assuming to be in UTC.')
        offset = None
    else:
        offset = df.index.tzinfo._utcoffset.total_seconds() / 60

    # Remove invalid MongoDB field characters
    df = df.rename(columns=lambda x: re.sub('\.', '', str(x)))
    index = _make_bson_column(df.index)
    columns = SON()
    for col in df.columns:
        columns[col] = _make_bson_column(df[col])

    nrows = len(df)
    binary_size = sum([columns[col]['size'] for col in df.columns])
    binary_size += index['size']
    compression_ratio = binary_size / mem_usage
    if binary_size > 0.95 * MAX_BSON_SIZE:
        msg = f'Binary data size is too large ({binary_size:,} / {compression_ratio:.1%})'
        raise InvalidBSON(msg, compression_ratio)
    logger.debug(f'{uid} document: {binary_size:,} bytes ({compression_ratio:.1%}), {nrows} rows')
    add_meta = {'nrows': nrows, 'binary_size': binary_size, 'utc_offset': offset}
    metadata = {**metadata, **add_meta}

    doc = SON([
        ('uid', uid),
        ('start', df.index[0]),
        ('end', df.index[-1]),
        ('metadata', metadata),
        ('index', index),
        ('columns', columns)])

    return doc


def make_bson_docs(uid, df, metadata, max_size=MAX_BSON_SIZE * 4) -> List[SON]:
    """Converts input DataFrame into BSON documents.

    Wrapper around ``_make_bson_doc``.
    Since BSON documents can't be larger than 16 MB, this function makes sure
    that the input DataFrame is properly split into smaller chunks that can be
    inserted into MongoDB. An initial compressibility factor of >4x (memory usage <64MB)
    is assumed and recursively updated if invalid BSON is generated.

    :param uid: Unique ID for the timeseries represented by the input DataFrame
    :param df: Input DataFrame
    :param metadata: Any BSON-able objects to be attached to document as metadata
    :param max_size: Initial maximum DataFrame memory usage
    :return: List of BSON documents
    """

    def split_dataframes(large_df: pd.DataFrame, size) -> List[pd.DataFrame]:
        mem_usage = large_df.memory_usage().sum()
        split_num = np.ceil(mem_usage / size)
        return np.array_split(large_df, split_num)

    docs = []
    for sub_df in split_dataframes(df, size=max_size):
        try:
            doc = _make_bson_doc(uid, sub_df, metadata)
            docs.append(doc)
        except InvalidBSON as e:
            new_max_size = np.floor(0.95 * MAX_BSON_SIZE / e.args[1])
            assert new_max_size > MAX_BSON_SIZE * 0.8
            logger.debug(f'Reducing max DataFrame split max_size to {new_max_size:,.0f}')
            return make_bson_docs(uid, df, metadata, max_size=new_max_size)
    return docs


def build_dataframe(docs: Iterable[SON]) -> pd.DataFrame:
    """Builds DataFrame from passed BSON documents.
    Input BSON document must match schema defined at ``make_bson_doc``.
    """
    dfs = []
    for doc in docs:
        index = pd.Index(_deserialize_array(doc['index']))
        index = index.tz_localize(pytz.FixedOffset(doc['metadata']['utc_offset'] or 0))
        columns = [_deserialize_array(col) for col in doc['columns'].values()]
        names = doc['columns'].keys()
        dfs.append(pd.DataFrame(index=index, data=dict(zip(names, columns))))

    return pd.concat(dfs, sort=False).sort_index()
