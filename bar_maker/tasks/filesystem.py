import os
from abc import ABCMeta, abstractmethod

from adlfs import AzureDatalakeFileSystem
from dotenv import load_dotenv

load_dotenv()
from bar_maker.core.factory import Registry
from gcsfs import GCSFileSystem
from s3fs import S3FileSystem

common_methods = [
    "async_impl",
    "asynchronous",
    "batch_size",
    "blocksize",
    "cachable",
    "cat",
    "cat_file",
    "cat_ranges",
    "checksum",
    "clear_instance_cache",
    "close_session",
    "copy",
    "cp",
    "cp_file",
    "created",
    "current",
    "default_block_size",
    "delete",
    "dircache",
    "disable_throttling",
    "disk_usage",
    "download",
    "du",
    "end_transaction",
    "exists",
    "expand_path",
    "find",
    "from_json",
    "get",
    "get_file",
    "get_mapper",
    "getxattr",
    "glob",
    "head",
    "info",
    "invalidate_cache",
    "isdir",
    "isfile",
    "lexists",
    "listdir",
    "loop",
    "ls",
    "makedir",
    "makedirs",
    "merge",
    "mkdir",
    "mkdirs",
    "modified",
    "move",
    "mv",
    "open",
    "open_async",
    "pipe",
    "pipe_file",
    "protocol",
    "put",
    "put_file",
    "read_block",
    "rename",
    "retries",
    "rm",
    "rm_file",
    "rmdir",
    "root_marker",
    "sep",
    "session",
    "sign",
    "size",
    "sizes",
    "split_path",
    "start_transaction",
    "stat",
    "storage_args",
    "storage_options",
    "tail",
    "to_json",
    "touch",
    "transaction",
    "ukey",
    "unstrip_protocol",
    "upload",
    "url",
    "walk",
]


class FSBase(metaclass=ABCMeta):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def prefix(self):
        """filepath prefix is required"""

    @abstractmethod
    def browser_link(self):
        """there should be a link provided for browser locations"""


class GCFS(FSBase):
    def __init__(self) -> None:
        super().__init__()

        self.agnostic = GCSFileSystem(os.environ.get("GCP_PROJECT", ""))
        self.native = ""

        for method in common_methods:
            setattr(self, method, getattr(self.agnostic, method))

    def prefix(self):
        return "gs://"

    def browser_link(self):
        raise NotImplementedError


class S3FS(FSBase):
    def __init__(self) -> None:
        super().__init__()

        self.agnostic = S3FileSystem(
            anon=False,
            use_ssl=False,
            client_kwargs={"endpoint_url": os.environ.get("S3_ENDPOINT", "")},
            key=os.environ.get("S3_KEY", ""),
            secret=os.environ.get("S3_SECRET", ""),
        )
        self.native = ""

        for method in common_methods:
            setattr(self, method, getattr(self.agnostic, method))

    def prefix(self):
        return "s3://"

    def browser_link(self):
        return "http://127.0.0.1:9001"


class AZFS(FSBase):
    def __init__(self) -> None:
        super().__init__()

        self.agnostic = AzureDatalakeFileSystem()
        self.native = ""

        for method in common_methods:
            setattr(self, method, getattr(self.agnostic, method))

    def prefix(self):
        raise NotImplementedError

    def browser_link(self):
        raise NotImplementedError


@Registry.register("get_filesystem")
class GetFileSystem:
    """
    Task to get an authenticated filesystem object from a signature string
    """

    def __init__(self) -> None:
        pass

    def execute(self, signature):
        try:
            return {"gcsfs": GCFS, "s3fs": S3FS, "azfs": AZFS}[signature]()
        except:
            raise ValueError(f"No such filesystem corresponding to {signature}")
