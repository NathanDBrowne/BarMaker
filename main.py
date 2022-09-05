from loguru import logger
from polars import DataFrame

from bar_maker import *
from bar_maker.core.factory import Registry


class Worker:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    def from_dict(cls, d):
        """task execute method"""
        return cls(**d)

    def run(self):
        ret = None
        for i, task in enumerate(self.tasks):
            task["args"]["fs_signature"] = self.fs_signature

            if isinstance(ret, DataFrame):
                task["args"]["df"] = ret
                print(ret)

            task_obj = Registry.get_task(task["name"])
            ret = task_obj.execute(**task["args"])


@logger.catch
def main():

    in_bucket = "tick-raw"
    out_bucket = "candlesticks"

    d = {
        "fs_signature": "s3fs",
        "tasks": [
            {
                "name": "read",
                "args": {
                    "filename": "eth_usdt/ccxt/ftx/eth_usdt_ftx_ETH_USDT_2020_05_01_0102_to_2020_08_04_1729.parquet",
                    "bucket": in_bucket,
                },
            },
            {
                "name": "aggregate",
                "args": {"unit": "milliseconds", "quantity": 60 * 60 * 1000},
                # "args": {"unit": "tick", "quantity": 1e8},
                # "args": {"unit": "dollar", "quantity": 1e4},
                # "args": {"unit": "tick", "quantity": 1e4},
            },
        ],
    }

    worker = Worker.from_dict(d)
    worker.run()


if __name__ == "__main__":
    main()
