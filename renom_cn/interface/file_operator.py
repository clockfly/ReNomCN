from abc import ABCMeta, abstractmethod
from future.utils import with_metaclass

from io import BytesIO, StringIO
import json
import pickle

import pandas as pd
import PIL
import PIL.Image
import pyarrow as pa
import pyarrow.parquet as pq
import xmltodict

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


# Pandas to File
class ParquetReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(ParquetReadWrite, self).__init__(fstype)

    def read(self, path):
        """paquet to pandas"""
        table = self.fs.read_parquet(path=path)
        return table.to_pandas()

    def write(self, path, data):
        """pandas to parquet"""
        table = pa.Table.from_pandas(data)
        pq.write_table(table, path, filesystem=self.fs)


class CSVReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(CSVReadWrite, self).__init__(fstype)

    def read(self, path):
        """csv to pandas"""
        with self.fs.open(path, mode='r') as f:
            data = pd.read_csv(StringIO(f.read()))
        return data

    def write(self, path, data):
        """pandas to csv"""
        with self.fs.open(path, mode='w') as f:
            s = StringIO()
            data.to_csv(s, index=False)
            f.write(s.getvalue())


class PickleReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(PickleReadWrite, self).__init__(fstype)

    def read(self, path):
        """pickle to pandas"""
        with self.fs.open(path, mode='rb') as f:
            data = pickle.load(f)
        return data

    def write(self, path, data):
        """pandas to pickle"""
        with self.fs.open(path, mode='wb') as f:
            pickle.dump(data, f)


# Dict to File
class JsonReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(JsonReadWrite, self).__init__(fstype)

    def read(self, path):
        """json to dict"""
        with self.fs.open(path, 'r') as f:
            data = json.load(f)
        return data

    def write(self, path, data):
        """dict to json"""
        with self.fs.open(path, 'w') as f:
            f.write(json.dumps(data))


class XmlReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(XmlReadWrite, self).__init__(fstype)

    def read(self, path):
        """xml to dict"""
        with self.fs.open(path, "rb") as f:
            d = xmltodict.parse(f, xml_attribs=True)
        return d

    def write(self, path, data):
        """dict to xml"""
        xmlstr = xmltodict.unparse(data, pretty=True)
        with self.fs.open(path, 'w') as f:
            f.write(xmlstr)


# PIL.Image to File
class ImageReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(ImageReadWrite, self).__init__(fstype)

    def read(self, path):
        with self.fs.open(path, mode='rb') as f:
            data = f.read()
        img = PIL.Image.open(BytesIO(data))
        return img

    def write(self, path, img, extensions):
        with self.fs.open(path, mode='wb') as f:
            b = BytesIO()
            img.save(b, extensions)
            f.write(b.getvalue())
