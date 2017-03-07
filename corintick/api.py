"""
High-level APIs for library users
"""

from .read import Reader
from .write import Writer

class Corintick:
    """
    To change db/bucket just simply reinstanciate object


    """

    def __init__(self, *args, **kwargs):
        self.reader = Reader(*args, **kwargs)
        self.writer = Writer(*args, **kwargs)

    def read(self, uid, *args, **kwargs):
        return self.reader.read(uid, *args, **kwargs)

    def write(self, uid, df, *args, **kwargs):
        self.writer.write(uid, df, *args, **kwargs)

    def list_series(self, uid, query=None, projection=None):
        pass