"""
Copyright 2019, Grid.

This source code is licensed under the ReNom Subscription Agreement, version 1.0.
ReNom Subscription Agreement Ver. 1.0 (https://www.renom.jp/info/license/index.html)
"""

import os
import types

import boto3
import s3fs
import pyarrow.filesystem as pafs


def rm(self, path):
    os.remove(path)


def s3mkdir(self, path):
    splitted_path = path.split("/")
    bucket_name = splitted_path[0]
    dirpath = "/".join(splitted_path[1:])
    dirpath = dirpath + "/"

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.put_object(Key=dirpath)


def get_fs(type="local"):
    """
    get filesystem function.

    Args:
        type : string, default "local".

    Returns:
        fs : pyarrow.filesystem.LocalFileSystem, pyarrow.filesystem.S3FSWrapper.
            pyarrow.filesystem module.

    Examples:
        >>> from renom_cn.api.filesystem import get_fs
        >>> fs = get_fs(type="local")
        >>> fs.ls(".")
        ['./a.txt']
        >>> fs.mkdir("testdir")
        >>> fs.ls(".")
        ['./a.txt', './testdir']
        >>> fs.exists("./a.txt")
        True
        >>> fs.exists("./b.txt")
        False
        >>> fs.isdir("./testdir")
        True
        >>> fs.rm("./a.txt")

        >>> fs = get_fs(type="s3")
        >>> fs.ls("my-bucket")
        FileNotFoundError: my-bucket  # if bucket is empty, raise FileNotFoundError
        >>> fs.mkdir("my-bucket/testdir")
        >>> fs.ls("my-bucket")
        ['my-bucket/testdir']
        >>> with fs.open('my-bucket/testdir/a.txt', 'wb') as f:
        ...     f.write(b"hello")
        ...
        5
        >>> with fs.open('my-bucket/testdir/a.txt', 'rb') as f:
        ...     print(f.read())
        ...
        b'hello'
        >>> fs.rm('my-bucket/testdir/a.txt')
    """
    if type == "s3":
        tmpfs = s3fs.S3FileSystem(anon=False)
        fs = pafs.S3FSWrapper(fs=tmpfs)
        fs.mkdir = types.MethodType(s3mkdir, fs)
    else:
        fs = pafs.LocalFileSystem()
        fs.rm = types.MethodType(rm, fs)
    return fs
