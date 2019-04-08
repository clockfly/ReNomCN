import s3fs
import pyarrow.filesystem as pafs


def get_fs(type="local"):
    if type == "s3":
        fs = s3fs.S3FileSystem(anon=False)
        return pafs.S3FSWrapper(fs=fs)
    else:
        return pafs.LocalFileSystem()
