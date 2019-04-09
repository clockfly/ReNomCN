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
    """
    Abstract class of file read/write module.
    """

    def __init__(self, fstype="local"):
        """
        initialize.

        Parameters
        ----------
            fstype : string, default "local".
                type of filesystem.
        """
        self.fs = filesystem.get_fs(fstype)

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def write(self):
        pass


# Pandas to File
class ParquetReadWrite(ReadWriteBase):
    """
    Class of Parquet file read/write module.
    """

    def __init__(self, fstype="local"):
        super(ParquetReadWrite, self).__init__(fstype)

    def read(self, path):
        """
        read parquet file.

        Parameters
        ----------
            path : string.
                filepath.

        Returns
        -------
            data : pandas.DataFrame.
        """
        table = self.fs.read_parquet(path=path)
        return table.to_pandas()

    def write(self, path, data):
        """
        Write pandas DataFrame to Parquet file.

        Parameters
        ----------
            path : string.
                filepath.

            data: pandas.DataFrame.
                file data.
        """
        table = pa.Table.from_pandas(data)
        pq.write_table(table, path, filesystem=self.fs)


class CSVReadWrite(ReadWriteBase):
    """
    Class of csv file read/write module.
    """

    def __init__(self, fstype="local"):
        super(CSVReadWrite, self).__init__(fstype)

    def read(self, path):
        """
        read csv file.

        Parameters
        ----------
            path : string.
                filepath.

        Returns
        -------
            data : pandas.DataFrame.
        """
        with self.fs.open(path, mode='r') as f:
            data = pd.read_csv(StringIO(f.read()))
        return data

    def write(self, path, data):
        """
        Write pandas DataFrame to csv file.

        Parameters
        ----------
            path : string.
                filepath.

            data: pandas.DataFrame.
                file data.
        """
        with self.fs.open(path, mode='w') as f:
            s = StringIO()
            data.to_csv(s, index=False)
            f.write(s.getvalue())


class PickleReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(PickleReadWrite, self).__init__(fstype)

    def read(self, path):
        """
        read pickle file.

        Parameters
        ----------
            path : string.
                filepath.

        Returns
        -------
            data : pandas.DataFrame.
        """
        with self.fs.open(path, mode='rb') as f:
            data = pickle.load(f)
        return data

    def write(self, path, data):
        """
        Write pandas DataFrame to pickle file.

        Parameters
        ----------
            path : string.
                filepath.

            data: pandas.DataFrame.
                file data.
        """
        with self.fs.open(path, mode='wb') as f:
            pickle.dump(data, f)


# Dict to File
class JsonReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(JsonReadWrite, self).__init__(fstype)

    def read(self, path):
        """
        read json file to dict.

        Parameters
        ----------
            path : string.
                filepath.

        Returns
        -------
            data : dict.
        """
        with self.fs.open(path, 'r') as f:
            data = json.load(f)
        return data

    def write(self, path, data):
        """
        write json file from dict.

        Parameters
        ----------
            path : string.
                filepath.

            data: dict.
                dict of json data.
        """
        with self.fs.open(path, 'w') as f:
            f.write(json.dumps(data))


class XmlReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(XmlReadWrite, self).__init__(fstype)

    def read(self, path):
        """
        read xml file to dict.

        Parameters
        ----------
            path : string.
                filepath.

        Returns
        -------
            data : dict.
        """
        with self.fs.open(path, "rb") as f:
            d = xmltodict.parse(f, xml_attribs=True)
        return d

    def write(self, path, data):
        """
        write xml file from dict.

        Parameters
        ----------
            path : string.
                filepath.

            data: dict.
                dict of xml data.
        """
        xmlstr = xmltodict.unparse(data, pretty=True)
        with self.fs.open(path, 'w') as f:
            f.write(xmlstr)


# PIL.Image to File
class ImageReadWrite(ReadWriteBase):
    def __init__(self, fstype="local"):
        super(ImageReadWrite, self).__init__(fstype)

    def read(self, path):
        """
        read img file to PIL.Image.

        Parameters
        ----------
            path : string.
                filepath.

        Returns
        -------
            img : PIL.Image.
        """
        with self.fs.open(path, mode='rb') as f:
            data = f.read()
        img = PIL.Image.open(BytesIO(data))
        return img

    def write(self, path, img, extension):
        """
        write img file from PIL.Image.

        Parameters
        ----------
            path : string.
                filepath.

            img : PIL.Image.
                image object.

            extension : string.
                file extension.

        """
        with self.fs.open(path, mode='wb') as f:
            b = BytesIO()
            img.save(b, extension)
            f.write(b.getvalue())
