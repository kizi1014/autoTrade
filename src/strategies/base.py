from abc import ABC, abstractmethod
import polars as pl


class Strategy(ABC):
    name = "base_strategy"

    @abstractmethod
    def compute_signals(self, data: pl.DataFrame) -> pl.DataFrame:
        pass
