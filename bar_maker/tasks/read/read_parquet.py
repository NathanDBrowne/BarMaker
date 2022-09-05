import polars as pl
from bar_maker.core.factory import TaskBase


class ReadParquet(TaskBase):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, fs, filename, bucket):
        with fs.open(f"{bucket}/{filename}") as f:
            df = pl.read_parquet(f)

        return df
