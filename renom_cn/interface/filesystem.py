import s3fs
import pyarrow.filesystem as pafs


def get_fs(type="local"):
    """
    get filesystem function.

    Parameters
    ----------
        type : string, default "local".

    Returns
    -------
        fs : pyarrow.filesystem.LocalFileSystem, pyarrow.filesystem.S3FSWrapper.
            pyarrow.filesystem module.
    """
    if type == "s3":
        fs = s3fs.S3FileSystem(anon=False)
        return pafs.S3FSWrapper(fs=fs)
    else:
        return pafs.LocalFileSystem()
