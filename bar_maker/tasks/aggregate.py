import polars as pl
from bar_maker.core.factory import Registry, TaskBase


@Registry.register("aggregate")
class Aggregate(TaskBase):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, df, unit="milliseconds", quantity=1000 * 60 * 60, **kwargs):
        """execute method for aggregation task

        Args:
            df (pl.DataFrame): a polars dataframe
            unit (str, optional): the unit by which to aggregate. Defaults to "milliseconds".
            quantity (float, optional): quantity of the unit on which to perform a groupby. Defaults to 1000*60*60.

        Returns:
            pl.DataFrame: an aggregated OHLCV + VWAP dataframe
        """
        agg_col = {
            "milliseconds": "timestamp",
            "tick": "tick",
            "dollar": "cost",
            "volume": "amount",
        }[unit]
        return self.get_normbars(df, agg_col, quantity)

    @staticmethod
    def get_normbars(df, column, grp_quant):
        """methods to normal aggregated sum bars from tick data

        Args:
            df (pl.DataFrame): a polars dataframe of tick level transactional data
            column (str): the column on which to perform groupbys
            grp_quant (float): quantity by which to group cumulatively

        Returns:
            pl.DataFrame: an aggregated OHLCV + VWAP dataframe
        """

        df = df.sort("timestamp")
        df = df.with_column(pl.arange(0, len(df)).alias("tick"))

        # if the column is not a timestamp we have to make a cumsum from it so we can make groupings
        if column == "timestamp":
            df = df.with_column(pl.col(column).alias(f"{column}_cumsum"))
        else:
            df = df.with_column(pl.col(column).cumsum().alias(f"{column}_cumsum"))

        # create aggregate data bins
        df = df.with_column(
            ((pl.col(f"{column}_cumsum") / grp_quant).floor()).alias(f"{column}_group")
        )

        # group the dataframe into required bins
        df = (
            df.groupby(f"{column}_group")
            .agg(
                [
                    pl.col("datetime").last().alias("datetime"),
                    pl.col("symbol").first().alias("symbol"),
                    pl.col("price").first().alias("open"),
                    pl.col("price").max().alias("high"),
                    pl.col("price").min().alias("low"),
                    pl.col("price").last().alias("close"),
                    pl.col("cost").sum().alias("value"),
                    pl.col("amount").sum().alias("volume"),
                ]
            )
            .sort("datetime")
        )

        # apply a win/loss column for green/red colouring
        df = df.with_column(
            pl.when(pl.col("open") < pl.col("close")).then(True).otherwise(False).alias("win")
        )
        return df

    @staticmethod
    def get_runs_bars():
        pass

    @staticmethod
    def get_imbalance_bars():
        pass
