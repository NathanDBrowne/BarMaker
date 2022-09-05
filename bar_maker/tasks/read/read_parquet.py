import polars as pl
from bar_maker.core.factory import TaskBase


class ReadParquet(TaskBase):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, fs, filename, bucket):
        """task to read a parquet file from a filesystem

        Args:
            fs (FSBase): an authenticated filesystem object
            filename (str): name of file to read
            bucket (str): name of bucket to read from

        Returns:
            pl.DataFrame: a polars dataframe read from a parquet
        """
        with fs.open(f"{bucket}/{filename}") as f:
            df = pl.read_parquet(f)

        return df
