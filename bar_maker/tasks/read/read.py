from bar_maker.core.factory import Registry, TaskBase
from bar_maker.tasks.filesystem import GetFileSystem
from bar_maker.tasks.read.read_parquet import ReadParquet


@Registry.register("read")
class Read(TaskBase):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, fs_signature, filename, **kwargs):
        fs_getter = GetFileSystem()
        fs = fs_getter.execute(fs_signature)

        ext = filename.split(".")[-1]

        if ext == "parquet":
            read_task = ReadParquet()

        return read_task.execute(fs, filename, **kwargs)
