import os
import pandas as pd
from pandas.util.testing import assert_frame_equal

from renom_cn.interface import file_operator


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
