"""
Copyright 2019, Grid.

This source code is licensed under the ReNom Subscription Agreement, version 1.0.
ReNom Subscription Agreement Ver. 1.0 (https://www.renom.jp/info/license/index.html)
"""

import pyarrow.filesystem as pafs

from renom_cn.api import filesystem


def test_fs_local():
    fs = filesystem.get_fs(type="local")
    assert(isinstance(fs, pafs.LocalFileSystem))


def test_fs_s3():
    fs = filesystem.get_fs(type="s3")
    assert(isinstance(fs, pafs.S3FSWrapper))
