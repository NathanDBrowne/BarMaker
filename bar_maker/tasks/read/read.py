from bar_maker.core.factory import Registry, TaskBase
from bar_maker.tasks.filesystem import GetFileSystem
from bar_maker.tasks.read.read_parquet import ReadParquet


@Registry.register("read")
class Read(TaskBase):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, fs_signature, filename, **kwargs):
        """dispatches a read task into an extension-specified read task

        Args:
            fs_signature (str): string identifier of the desired filesystem
            filename (str): name of file to read from the filesystem

        Returns:
            pl.DataFrame: polars dataframe of the read file
        """
        fs_getter = GetFileSystem()
        fs = fs_getter.execute(fs_signature)

        ext = filename.split(".")[-1]

        if ext == "parquet":
            read_task = ReadParquet()

        return read_task.execute(fs, filename, **kwargs)
