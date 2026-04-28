import polars as pl
from src.strategies.base import Strategy


class MyStrategy(Strategy):
    name = "my_strategy"

    def __init__(self, param1=10, param2=30):
        super().__init__()
        self.param1 = param1
        self.param2 = param2

    def compute_signals(self, data: pl.DataFrame) -> pl.DataFrame:
        data = data.sort(["code", "date"])
        data = data.with_columns([
            pl.col("close").rolling_mean(self.param1).over("code").alias("ma1"),
            pl.col("close").rolling_mean(self.param2).over("code").alias("ma2"),
        ])
        data = data.with_columns(
            pl.when(pl.col("ma1") > pl.col("ma2"))
            .then(1)
            .otherwise(0)
            .alias("signal")
        )
        return data.select(["date", "code", "signal"]).drop_nulls()
