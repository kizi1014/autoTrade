import polars as pl
from src.strategies.base import Strategy


class MacdRsiStrategy(Strategy):
    name = "macd_rsi"

    def __init__(self, rsi_period=14, rsi_threshold=50):
        super().__init__()
        self.rsi_period = rsi_period
        self.rsi_threshold = rsi_threshold

    def compute_signals(self, data: pl.DataFrame) -> pl.DataFrame:
        data = data.sort(["code", "date"])

        data = data.with_columns([
            pl.col("close").ewm_mean(span=12, adjust=False).over("code").alias("ema12"),
            pl.col("close").ewm_mean(span=26, adjust=False).over("code").alias("ema26"),
        ])
        data = data.with_columns(
            (pl.col("ema12") - pl.col("ema26")).alias("dif")
        )
        data = data.with_columns(
            pl.col("dif").ewm_mean(span=9, adjust=False).over("code").alias("dea")
        )

        data = data.with_columns(
            (pl.col("close") - pl.col("close").shift(1)).over("code").alias("price_change")
        )
        data = data.with_columns([
            pl.when(pl.col("price_change") > 0).then(pl.col("price_change")).otherwise(0).alias("gain"),
            pl.when(pl.col("price_change") < 0).then(-pl.col("price_change")).otherwise(0).alias("loss"),
        ])
        data = data.with_columns([
            pl.col("gain").ewm_mean(span=self.rsi_period, adjust=False).over("code").alias("avg_gain"),
            pl.col("loss").ewm_mean(span=self.rsi_period, adjust=False).over("code").alias("avg_loss"),
        ])
        data = data.with_columns(
            (100 - 100 / (1 + pl.col("avg_gain") / pl.col("avg_loss").clip(lower_bound=0.001))).alias("rsi")
        )

        data = data.with_columns(
            pl.when(
                (pl.col("dif") > pl.col("dea")) &
                (pl.col("rsi") > self.rsi_threshold)
            )
            .then(1)
            .otherwise(0)
            .alias("signal")
        )

        return data.select(["date", "code", "signal"]).drop_nulls()
