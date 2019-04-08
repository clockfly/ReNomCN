from abc import ABCMeta, abstractmethod
from future.utils import with_metaclass

from io import BytesIO, StringIO
import pickle

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from renom_cn.interface import filesystem


class ReadWriteBase(with_metaclass(ABCMeta, object)):
    def __init__(self, fstype="local"):
        self.fs = filesystem.get_fs(fstype)

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def write(self):
        pass


class ParquetReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(ParquetReadWrite, self).__init__(fstype)

    def read(self, path):
        table = self.fs.read_parquet(path=path)
        return table.to_pandas()

    def write(self, path, data):
        table = pa.Table.from_pandas(data)
        pq.write_table(table, path, filesystem=self.fs)


class CSVReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(CSVReadWrite, self).__init__(fstype)

    def read(self, path):
        with self.fs.open(path, mode='r') as f:
            data = pd.read_csv(StringIO(f.read()))
        return data

    def write(self, path, data):
        with self.fs.open(path, mode='w') as f:
            s = StringIO()
            data.to_csv(s, index=False)
            f.write(s.getvalue())


class PickleReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(PickleReadWrite, self).__init__(fstype)

    def read(self, path):
        with self.fs.open(path, mode='rb') as f:
            data = pickle.load(f)
        return data

    def write(self, path, data):
        with self.fs.open(path, mode='wb') as f:
            pickle.dump(data, f)
