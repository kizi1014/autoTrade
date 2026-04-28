import polars as pl
from src.data.storage import DataStorage


class SignalGenerator:
    def __init__(self, config, strategy):
        self.config = config
        self.strategy = strategy

    def generate(self, data: pl.DataFrame):
        signals = self.strategy.compute_signals(data)
        latest = signals.sort(["date", "code"]).group_by("code").agg(
            pl.last("date"), pl.last("signal")
        )
        buy = latest.filter(pl.col("signal") == 1)
        sell = latest.filter(pl.col("signal") == 0)
        return {"buy": buy, "sell": sell, "latest": latest}
