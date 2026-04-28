import polars as pl
from src.strategies.base import Strategy


class MACrossStrategy(Strategy):
    name = "ma_cross"

    def __init__(self, short_period=5, long_period=20):
        super().__init__()
        self.short_period = short_period
        self.long_period = long_period

    def compute_signals(self, data: pl.DataFrame) -> pl.DataFrame:
        data = data.sort(["code", "date"])
        data = data.with_columns([
            pl.col("close").rolling_mean(self.short_period).over("code").alias("ma_short"),
            pl.col("close").rolling_mean(self.long_period).over("code").alias("ma_long"),
        ])
        data = data.with_columns(
            pl.when(pl.col("ma_short") > pl.col("ma_long"))
            .then(1)
            .otherwise(0)
            .alias("signal")
        )
        return data.select(["date", "code", "signal"]).drop_nulls()
