from dataclasses import dataclass, field
import polars as pl
from .metrics import calculate_metrics


@dataclass
class BacktestResult:
    equity_curve: list = field(default_factory=list)
    trades: list = field(default_factory=list)
    metrics: dict = field(default_factory=dict)


class BacktestEngine:
    def __init__(self, config):
        c = config["backtest"]
        self.initial_capital = float(c["initial_capital"])
        self.commission_rate = float(c["commission_rate"])
        self.stamp_duty = float(c["stamp_duty"])

    def run(self, data: pl.DataFrame, strategy) -> BacktestResult:
        merged = self._prepare(data, strategy)
        return self._simulate(merged)

    def _prepare(self, data: pl.DataFrame, strategy):
        data = data.sort(["code", "date"])
        data = data.with_columns(
            (pl.col("close") / pl.col("close").shift(1) - 1).over("code").alias("returns")
        ).drop_nulls()

        signals = strategy.compute_signals(data)
        merged = data.join(signals, on=["date", "code"], how="inner").sort(["code", "date"])
        merged = merged.with_columns(
            pl.col("signal").shift(1).over("code").fill_null(0).alias("position")
        ).drop_nulls()
        return merged

    def _simulate(self, data):
        dates = data["date"].unique().sort()
        equity = self.initial_capital
        prev_weights = {}
        equity_curve = []
        all_trades = []

        for date in dates:
            day = data.filter(pl.col("date") == date)
            day_returns = {r[day.columns.index("code")]: r[day.columns.index("returns")] for r in day.iter_rows()}
            day_positions = {r[day.columns.index("code")]: r[day.columns.index("position")] for r in day.iter_rows()}

            active = {k: v for k, v in day_positions.items() if v > 0}
            n_active = len(active)
            target_weights = {k: 1.0 / n_active for k in active} if n_active > 0 else {}

            all_codes = set(prev_weights.keys()) | set(target_weights.keys())
            turnover_weight = sum(
                abs(target_weights.get(c, 0) - prev_weights.get(c, 0)) for c in all_codes
            )

            tx_cost_ratio = turnover_weight * (self.commission_rate + self.stamp_duty)
            port_return = sum(
                weight * day_returns.get(code, 0)
                for code, weight in prev_weights.items()
            )

            equity = equity * (1 + port_return) * (1 - tx_cost_ratio)
            equity_curve.append((date, round(equity, 2)))

            for code in target_weights:
                if code not in prev_weights or prev_weights[code] == 0:
                    row = day.filter(pl.col("code") == code)
                    all_trades.append({
                        "date": date, "code": code,
                        "action": "buy",
                        "price": row["close"].item(),
                    })
            for code in list(prev_weights.keys()):
                if code not in target_weights and prev_weights[code] > 0:
                    row = day.filter(pl.col("code") == code)
                    if len(row) > 0:
                        all_trades.append({
                            "date": date, "code": code,
                            "action": "sell",
                            "price": row["close"].item(),
                        })

            prev_weights = target_weights

        metrics = calculate_metrics(equity_curve, self.initial_capital)
        return BacktestResult(
            equity_curve=equity_curve,
            trades=all_trades,
            metrics=metrics,
        )
