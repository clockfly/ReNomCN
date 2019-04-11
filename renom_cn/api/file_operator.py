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

from renom_cn.api import filesystem


class ReadWriteBase(with_metaclass(ABCMeta, object)):
    """
    Abstract class of file read/write module.
    """

    def __init__(self, fstype="local"):
        """
        initialize filesystem attribute.

        Args:
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

    Examples:
        >>> import os
        >>> import pandas as pd
        >>> from renom_cn.api.file_operator import ParquetReadWrite
        >>> test_data = {"col1": [1, 2], "col2": [3, 4]}
        >>> test_df = pd.DataFrame(data=test_data)
        >>> test_df
           col1  col2
        0     1     3
        1     2     4
        >>> rw = ParquetReadWrite(fstype="local")
        >>> filepath = "/tmp/data.parquet"
        >>> rw.write(filepath, test_df)
        >>> os.path.exists(filepath)
        True
        >>> rw.read(filepath)
           col1  col2
        0     1     3
        1     2     4
        >>> rw.fs.rm(filepath)
        >>> os.path.exists(filepath)
        False
    """

    def __init__(self, fstype="local"):
        """
        initialize filesystem attribute.

        Args:
            fstype : string, default "local".
                type of filesystem.
        """
        super(ParquetReadWrite, self).__init__(fstype)

    def read(self, path):
        """
        read parquet file.

        Args:
            path : string.
                filepath.

        Returns:
            data : pandas.DataFrame.
        """
        table = self.fs.read_parquet(path=path)
        return table.to_pandas()

    def write(self, path, data):
        """
        Write pandas DataFrame to Parquet file.

        Args:
            path : string.
                filepath.

            data: pandas.DataFrame.
        """
        table = pa.Table.from_pandas(data)
        pq.write_table(table, path, filesystem=self.fs)


class CSVReadWrite(ReadWriteBase):
    """
    Class of csv file read/write module.

    Examples:
        >>> import os
        >>> import pandas as pd
        >>> from renom_cn.api.file_operator import CSVReadWrite
        >>> test_data = {"col1": [1, 2], "col2": [3, 4]}
        >>> test_df = pd.DataFrame(data=test_data)
        >>> test_df
           col1  col2
        0     1     3
        1     2     4
        >>> rw = CSVReadWrite(fstype="local")
        >>> filepath = "/tmp/data.csv"
        >>> rw.write(filepath, test_df)
        >>> os.path.exists(filepath)
        True
        >>> rw.read(filepath)
           col1  col2
        0     1     3
        1     2     4
        >>> rw.fs.rm(filepath)
        >>> os.path.exists(filepath)
        False
    """

    def __init__(self, fstype="local"):
        """
        initialize filesystem attribute.

        Args:
            fstype : string, default "local".
                type of filesystem.
        """
        super(CSVReadWrite, self).__init__(fstype)

    def read(self, path):
        """
        read csv file.

        Args:
            path : string.
                filepath.

        Returns:
            data : pandas.DataFrame.
        """
        with self.fs.open(path, mode='r') as f:
            data = pd.read_csv(StringIO(f.read()))
        return data

    def write(self, path, data):
        """
        Write pandas DataFrame to csv file.

        Args:
            path : string.
                filepath.

            data: pandas.DataFrame.
        """
        with self.fs.open(path, mode='w') as f:
            s = StringIO()
            data.to_csv(s, index=False)
            f.write(s.getvalue())


class PickleReadWrite(ReadWriteBase):
    """
    Class of pickle file read/write module.

    Examples:
        >>> import os
        >>> import pandas as pd
        >>> from renom_cn.api.file_operator import PickleReadWrite
        >>> test_data = {"col1": [1, 2], "col2": [3, 4]}
        >>> test_df = pd.DataFrame(data=test_data)
        >>> test_df
           col1  col2
        0     1     3
        1     2     4
        >>> rw = PickleReadWrite(fstype="local")
        >>> filepath = "/tmp/data.pickle"
        >>> rw.write(filepath, test_df)
        >>> os.path.exists(filepath)
        True
        >>> rw.read(filepath)
           col1  col2
        0     1     3
        1     2     4
        >>> rw.fs.rm(filepath)
        >>> os.path.exists(filepath)
        False
    """
    def __init__(self, fstype="local"):
        """
        initialize filesystem attribute.

        Args:
            fstype : string, default "local".
                type of filesystem.
        """
        super(PickleReadWrite, self).__init__(fstype)

    def read(self, path):
        """
        read pickle file.

        Args:
            path : string.
                filepath.

        Returns:
            data : pandas.DataFrame.
        """
        with self.fs.open(path, mode='rb') as f:
            data = pickle.load(f)
        return data

    def write(self, path, data):
        """
        Write pandas DataFrame to pickle file.

        Args:
            path : string.
                filepath.

            data: pandas.DataFrame.
        """
        with self.fs.open(path, mode='wb') as f:
            pickle.dump(data, f)


# Dict to File
class JsonReadWrite(ReadWriteBase):
    """
    Class of json file read/write module.

    Examples:
        >>> import os
        >>> from renom_cn.api.file_operator import JsonReadWrite
        >>> test_dict = {"test": {"key1": "val1", "key2": "val2"} }
        >>> rw = JsonReadWrite(fstype="local")
        >>> filepath = "/tmp/data.json"
        >>> rw.write(filepath, test_dict)
        >>> os.path.exists(filepath)
        True
        >>> rw.read(filepath)
        {'test': {'key1': 'val1', 'key2': 'val2'}}
        >>> rw.fs.rm(filepath)
        >>> os.path.exists(filepath)
        False
    """
    def __init__(self, fstype="local"):
        """
        initialize filesystem attribute.

        Args:
            fstype : string, default "local".
                type of filesystem.
        """
        super(JsonReadWrite, self).__init__(fstype)

    def read(self, path):
        """
        read json file to dict.

        Args:
            path : string.
                filepath.

        Returns:
            data : dict.
        """
        with self.fs.open(path, 'r') as f:
            data = json.load(f)
        return data

    def write(self, path, data):
        """
        write json file from dict.

        Args:
            path : string.
                filepath.

            data: dict.
                dict of json data.
        """
        with self.fs.open(path, 'w') as f:
            f.write(json.dumps(data))


class XmlReadWrite(ReadWriteBase):
    """
    Class of xml file read/write module.

    Examples:
        >>> import os
        >>> from renom_cn.api.file_operator import XmlReadWrite
        >>> test_dict = {"test": {"key1": "val1", "key2": "val2"} }
        >>> rw = XmlReadWrite(fstype="local")
        >>> filepath = "/tmp/data.xml"
        >>> rw.write(filepath, test_dict)
        >>> os.path.exists(filepath)
        True
        >>> rw.read(filepath)
        OrderedDict([('test', OrderedDict([('key1', 'val1'), ('key2', 'val2')]))])
        >>> rw.fs.rm(filepath)
        >>> os.path.exists(filepath)
        False
    """
    def __init__(self, fstype="local"):
        """
        initialize filesystem attribute.

        Args:
            fstype : string, default "local".
                type of filesystem.
        """
        super(XmlReadWrite, self).__init__(fstype)

    def read(self, path):
        """
        read xml file to dict.

        Args:
            path : string.
                filepath.

        Returns:
            data : dict.
        """
        with self.fs.open(path, "rb") as f:
            d = xmltodict.parse(f, xml_attribs=True)
        return d

    def write(self, path, data):
        """
        write xml file from dict.

        Args:
            path : string.
                filepath.

            data: OrderedDict.
                OrderedDict of xml data.
        """
        xmlstr = xmltodict.unparse(data, pretty=True)
        with self.fs.open(path, 'w') as f:
            f.write(xmlstr)


# PIL.Image to File
class ImageReadWrite(ReadWriteBase):
    """
    Class of image file read/write module.

    Examples:
        >>> import os
        >>> import numpy as np
        >>> import PIL.Image
        >>> from renom_cn.api.file_operator import ImageReadWrite
        >>> test_img_array = np.random.randint(0, 255, (128, 128, 3)).astype(np.uint8)
        >>> test_img = PIL.Image.fromarray(np.uint8(test_img_array))
        >>> rw = ImageReadWrite(fstype="local")
        >>> filepath = "/tmp/data.png"
        >>> rw.write(filepath, test_img, "png")
        >>> os.path.exists(filepath)
        True
        >>> rw.read(filepath)
        <PIL.PngImagePlugin.PngImageFile image mode=RGB size=128x128 at 0x11314F240>
        >>> rw.fs.rm(filepath)
        >>> os.path.exists(filepath)
        False
    """
    def __init__(self, fstype="local"):
        """
        initialize filesystem attribute.

        Args:
            fstype : string, default "local".
                type of filesystem.
        """
        super(ImageReadWrite, self).__init__(fstype)

    def read(self, path):
        """
        read img file to PIL.Image.

        Args:
            path : string.
                filepath.

        Returns:
            img : PIL.Image.
        """
        with self.fs.open(path, mode='rb') as f:
            data = f.read()
        img = PIL.Image.open(BytesIO(data))
        return img

    def write(self, path, img, extension):
        """
        write img file from PIL.Image.

        Args:
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
