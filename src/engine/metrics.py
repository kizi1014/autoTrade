import numpy as np


def calculate_metrics(equity_curve, initial_capital):
    if len(equity_curve) < 2:
        return {"error": "数据不足", "total_return": 0.0, "sharpe": 0.0}

    equity_values = [e for _, e in equity_curve]
    total_return = equity_values[-1] / initial_capital - 1
    n_days = len(equity_values) - 1
    years = n_days / 252

    annual_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
    daily_returns = np.diff(equity_values) / np.array(equity_values[:-1])
    annual_vol = np.std(daily_returns, ddof=1) * np.sqrt(252) if len(daily_returns) > 1 else 0

    risk_free = 0.02
    sharpe = (annual_return - risk_free) / annual_vol if annual_vol > 0 else 0

    peak = np.maximum.accumulate(equity_values)
    drawdown = (np.array(equity_values) - peak) / peak
    max_drawdown = np.min(drawdown)

    wins = np.sum(daily_returns > 0)
    win_rate = wins / len(daily_returns) if len(daily_returns) > 0 else 0

    return {
        "total_return": total_return,
        "annual_return": annual_return,
        "annual_vol": annual_vol,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "win_rate": win_rate,
        "n_trading_days": n_days,
    }
