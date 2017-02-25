"""
Where all the compression realted stuff should go
"""
import lz4
import hashlib
import pickle

from bson import Binary, SON
import pandas as pd

from .utils import make_logger

logger = make_logger(__name__)

def compress_columns(col: pd.Series) -> dict:
    """
    Compresses column/index and returns a dictionary
    with binary column data and some metadata
    :param col: Input column data
    :return: Compressed column data dictionary
    """
    data = Binary(lz4.compressHC(pickle.dumps(col.values, protocol=pickle.HIGHEST_PROTOCOL)))
    sha1 = Binary(hashlib.sha1(data).digest())
    dtype = str(col.dtype)
    size = len(data)
    return {'data': data, 'dtype': dtype, 'sha1': sha1, 'size': size}

def make_bson(df, metadata=None) -> SON:
    """
    Takes a DataFrame and makes a BSON document ready to be inserted
    into MongoDB. Input DataFrame is assumed to have a sorted DatetimeIndex,
     however that is not strictly mandatory.
    Output BSON document can't be larger than 16 MB, so input DataFrame
    should take that into consideration. Use `pandas.DataFrame.memory_usage().sum()`\
    to see total memory consumption of input. A maximum memory usage of 64MB should
    be fine, but your milege may vary depending on compressibility of input.
    :param df: Input DataFrame
    :param metadata: Any BSON-able dictionary containing metadata to be attached to document
    :return: BSON document
    """
    if df.index.__class__.__name__ != 'DatetimeIndex':
        logger.warning('DataFrame index is not DatetimeIndex')
    index = compress_columns(df.index)
    columns = SON()
    for col in df.columns:
        columns[col] = compress_columns(df[col])
    total_size = sum([columns[col]['size'] for col in df.columns])
    logger.info('Document size: {:,} bytes'.format(total_size))
    doc = SON([
        ('start', df.index[0]),
        ('end', df.index[-1]),
        ('nrows', len(df)),
        ('index', index),
        ('columns', columns)])
    if metadata:
        doc.update(metadata)
    return doc