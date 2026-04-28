import click
import yaml
import polars as pl
from pathlib import Path
from datetime import datetime
from rich.console import Console
from rich.table import Table
from src.data.fetcher import DataFetcher
from src.data.storage import DataStorage
from src.engine.backtest import BacktestEngine
from src.engine.metrics import calculate_metrics
from src.strategies import load_strategy
from src.signal.generator import SignalGenerator
from src.signal.notifier import DingTalkNotifier
from src.monitor import RealtimeMonitor

console = Console()


def load_config():
    with open("config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


@click.group()
def cli():
    pass


@cli.command()
def update():
    config = load_config()
    storage = DataStorage(config)
    fetcher = DataFetcher(config, storage)
    with console.status("[cyan]正在更新数据..."):
        fetcher.update_all()
    console.print("[green]数据更新完成![/green]")


@cli.command()
@click.argument("strategy_name", default="ma_cross")
def backtest(strategy_name):
    config = load_config()
    storage = DataStorage(config)
    strategy = load_strategy(strategy_name)
    engine = BacktestEngine(config)

    with console.status("[cyan]正在加载数据..."):
        data = storage.load_all()
    if data.is_empty():
        console.print("[red]未找到数据，请先运行 update[/red]")
        return

    with console.status("[cyan]正在运行回测..."):
        result = engine.run(data, strategy)

    m = result.metrics
    table = Table(title=f"回测结果 - {strategy_name}")
    table.add_column("指标", style="cyan")
    table.add_column("数值", style="green")
    table.add_row("累计收益率", f"{m['total_return']:.2%}")
    table.add_row("年化收益率", f"{m['annual_return']:.2%}")
    table.add_row("年化波动率", f"{m['annual_vol']:.2%}")
    table.add_row("夏普比率", f"{m['sharpe']:.2f}")
    table.add_row("最大回撤", f"{m['max_drawdown']:.2%}")
    table.add_row("胜率", f"{m['win_rate']:.2%}")
    table.add_row("交易次数", str(len(result.trades)))
    console.print(table)


@cli.command()
@click.argument("strategy_name", default="ma_cross")
@click.option("--notify", is_flag=True, help="通过钉钉发送信号")
def signal(strategy_name, notify):
    config = load_config()
    storage = DataStorage(config)
    strategy = load_strategy(strategy_name)

    with console.status("[cyan]正在加载数据..."):
        data = storage.load_all()
    if data.is_empty():
        console.print("[red]未找到数据，请先运行 update[/red]")
        return

    with console.status("[cyan]正在生成信号..."):
        generator = SignalGenerator(config, strategy)
        result = generator.generate(data)

    latest_date = data["date"].max()
    today_signals = result["latest"].sort("code")

    table = Table(title=f"交易信号 - {latest_date}")
    table.add_column("代码", style="cyan")
    table.add_column("信号", style="green")
    signal_map = {1: "[green]买入[/green]", 0: "[yellow]持有/卖出[/yellow]"}
    for row in today_signals.iter_rows():
        table.add_row(str(row[0]), signal_map.get(row[2], str(row[2])))
    console.print(table)

    if notify:
        cfg = config.get("notify", {})
        if cfg.get("enabled") and cfg.get("webhook"):
            stock_list = storage.list_stocks_with_names()
            name_map = dict(stock_list)
            notifier = DingTalkNotifier(cfg["webhook"], cfg.get("secret"))
            buy_items = [
                {"code": r[0], "name": name_map.get(r[0], r[0])}
                for r in today_signals.filter(pl.col("signal") == 1).iter_rows()
            ]
            sell_items = [
                {"code": r[0], "name": name_map.get(r[0], r[0])}
                for r in today_signals.filter(pl.col("signal") == 0).iter_rows()
            ]
            resp = notifier.send_signal(buy_items, sell_items, str(latest_date))
            console.print(f"[green]钉钉通知已发送: {resp}[/green]")
        else:
            console.print("[yellow]钉钉通知未配置，请在 config.yaml 中设置 notify[/yellow]")
    else:
        console.print("[dim]使用 --notify 可通过钉钉发送信号[/dim]")


@cli.command()
@click.argument("strategy_name", default="ma_cross")
def monitor(strategy_name):
    config = load_config()
    storage = DataStorage(config)
    strategy = load_strategy(strategy_name)

    cfg = config.get("notify", {})
    notifier = None
    if cfg.get("enabled") and cfg.get("webhook"):
        notifier = DingTalkNotifier(cfg["webhook"], cfg.get("secret"))

    m = RealtimeMonitor(config, storage, strategy)
    console.print("[green]启动盘中实时监测（交易时段 9:30-11:30, 13:00-15:00）[/green]")
    console.print("[green]信号变化时将自动推送钉钉通知[/green]")
    try:
        m.run(notifier)
    except KeyboardInterrupt:
        console.print("\n[yellow]监测已停止[/yellow]")


if __name__ == "__main__":
    cli()
