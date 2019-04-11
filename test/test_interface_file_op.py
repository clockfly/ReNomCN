"""
Copyright 2019, Grid.

This source code is licensed under the ReNom Subscription Agreement, version 1.0.
ReNom Subscription Agreement Ver. 1.0 (https://www.renom.jp/info/license/index.html)
"""

import os
import numpy as np
from numpy.testing import assert_array_equal
import pandas as pd
from pandas.util.testing import assert_frame_equal
import PIL.Image

from renom_cn.api import file_operator


test_data = {"col1": [1, 2], "col2": [3, 4]}
test_df = pd.DataFrame(data=test_data)


class TestParquetReadWrite(object):
    @classmethod
    def setup_class(self):
        self.path = "files/data.pq"
        self.rw = file_operator.ParquetReadWrite(fstype="local")

    @classmethod
    def teardown_class(self):
        os.remove(self.path)

    def test_write(self):
        self.rw.write(self.path, test_df)

    def test_read(self):
        data = self.rw.read(self.path)
        assert_frame_equal(data, test_df)


class TestCSVReadWrite(object):
    @classmethod
    def setup_class(self):
        self.path = "files/data.csv"
        self.rw = file_operator.CSVReadWrite(fstype="local")

    @classmethod
    def teardown_class(self):
        os.remove(self.path)

    def test_write(self):
        self.rw.write(self.path, test_df)

    def test_read(self):
        data = self.rw.read(self.path)
        assert_frame_equal(data, test_df)


class TestPickleReadWrite(object):
    @classmethod
    def setup_class(self):
        self.path = "files/data.pkl"
        self.rw = file_operator.PickleReadWrite(fstype="local")

    @classmethod
    def teardown_class(self):
        os.remove(self.path)

    def test_write(self):
        self.rw.write(self.path, test_df)

    def test_read(self):
        data = self.rw.read(self.path)
        assert_frame_equal(data, test_df)


test_dict = {
    "test": {"key1": "val1", "key2": "val2"}
}


class TestJsonReadWrite(object):
    @classmethod
    def setup_class(self):
        self.path = "files/data.json"
        self.rw = file_operator.JsonReadWrite(fstype="local")

    @classmethod
    def teardown_class(self):
        os.remove(self.path)

    def test_write(self):
        self.rw.write(self.path, test_dict)

    def test_read(self):
        data = self.rw.read(self.path)
        assert data == test_dict


class TestXmlReadWrite(object):
    @classmethod
    def setup_class(self):
        self.path = "files/data.xml"
        self.rw = file_operator.XmlReadWrite(fstype="local")

    @classmethod
    def teardown_class(self):
        os.remove(self.path)

    def test_write(self):
        self.rw.write(self.path, test_dict)

    def test_read(self):
        data = self.rw.read(self.path)
        assert data == test_dict


test_img_array = np.random.randint(0, 255, (128, 128, 3)).astype(np.uint8)
test_img = PIL.Image.fromarray(np.uint8(test_img_array))


class TestImageReadWrite(object):
    @classmethod
    def setup_class(self):
        self.path = "files/data.png"
        self.rw = file_operator.ImageReadWrite(fstype="local")

    @classmethod
    def teardown_class(self):
        os.remove(self.path)

    def test_write(self):
        self.rw.write(self.path, test_img, "png")

    def test_read(self):
        data = self.rw.read(self.path)
        data_array = np.array(data)
        assert_array_equal(data_array, test_img_array)
