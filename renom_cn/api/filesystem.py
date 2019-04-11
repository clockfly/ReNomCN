import s3fs
import pyarrow.filesystem as pafs


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

        >>> fs = get_fs(type="s3")
        >>> fs.ls("grid-cntest")
        ['grid-cntest/testdir']
        >>> with fs.open('grid-cntest/testdir/a.txt', 'wb') as f:
        ...     f.write(b"hello")
        ...
        5
        >>> with fs.open('grid-cntest/testdir/a.txt', 'rb') as f:
        ...     print(f.read())
        ...
        b'hello'
        >>>
    """
    if type == "s3":
        fs = s3fs.S3FileSystem(anon=False)
        return pafs.S3FSWrapper(fs=fs)
    else:
        return pafs.LocalFileSystem()
