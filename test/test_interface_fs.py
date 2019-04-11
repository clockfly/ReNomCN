import pyarrow.filesystem as pafs

from renom_cn.api import filesystem


def test_fs_local():
    fs = filesystem.get_fs(type="local")
    assert(isinstance(fs, pafs.LocalFileSystem))


def test_fs_s3():
    fs = filesystem.get_fs(type="s3")
    assert(isinstance(fs, pafs.S3FSWrapper))
