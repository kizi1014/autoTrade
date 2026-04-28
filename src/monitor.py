import time
import polars as pl
import akshare as ak
from datetime import datetime, date, time as dtime
from datetime import timedelta
from src.network import HEADERS


class RealtimeMonitor:
    def __init__(self, config, storage, strategy):
        self.config = config
        self.storage = storage
        self.strategy = strategy
        m = config.get("monitor", {})
        self.interval = m.get("interval", 30)
        self.alert_cooldown = m.get("alert_cooldown", 300)
        self.stock_pool = config["data"].get("stock_pool", [])
        self.daily_data = None
        self.last_signals = {}
        self.last_alert_time = {}

    def is_trading_time(self):
        now = datetime.now()
        if now.weekday() >= 5:
            return False
        t = now.time()
        return (dtime(9, 30) <= t <= dtime(11, 30)) or (dtime(13, 0) <= t <= dtime(15, 0))

    def _next_trading_start(self):
        now = datetime.now()
        today = now.date()
        t = now.time()
        if t < dtime(9, 30):
            return datetime.combine(today, dtime(9, 30))
        if dtime(11, 30) < t < dtime(13, 0):
            return datetime.combine(today, dtime(13, 0))
        if t >= dtime(15, 0):
            d = today + timedelta(days=1)
            while d.weekday() >= 5:
                d += timedelta(days=1)
            return datetime.combine(d, dtime(9, 30))
        return now

    def init(self):
        self.daily_data = self.storage.load_all()
        if self.daily_data.is_empty():
            raise RuntimeError("无历史数据，请先运行 python run.py update")
        signals = self._compute_signals(self.daily_data)
        for r in signals.iter_rows():
            self.last_signals[r[0]] = r[1]

    def _compute_signals(self, data):
        raw = self.strategy.compute_signals(data)
        return raw.group_by("code").agg(pl.last("signal")).sort("code")

    def _fetch_realtime(self):
        df = ak.stock_zh_a_spot_em()
        cols = {
            "代码": "code", "名称": "name", "最新价": "price",
            "今开": "open", "最高": "high", "最低": "low",
            "成交量": "volume", "昨收": "pre_close",
        }
        keep = [c for c in cols if c in df.columns]
        data = pl.from_pandas(df[keep]).rename({k: cols[k] for k in keep})
        for c in ["price", "open", "high", "low", "volume", "pre_close"]:
            if c in data.columns:
                data = data.with_columns(pl.col(c).cast(pl.Float64))
        if self.stock_pool:
            data = data.filter(pl.col("code").is_in(self.stock_pool))
        return data

    def run(self, notifier=None):
        self.init()
        while True:
            try:
                if self.is_trading_time():
                    self._check(notifier)
                else:
                    next_ts = self._next_trading_start()
                    remaining = (next_ts - datetime.now()).total_seconds()
                    if remaining > 0:
                        mins = int(remaining // 60)
                        print(f"[Monitor] 非交易时间，{mins} 分钟后恢复")
                        time.sleep(min(remaining, 60))
                        continue
            except Exception as e:
                print(f"[Monitor] 错误: {e}")
            time.sleep(self.interval)

    def _check(self, notifier):
        realtime = self._fetch_realtime()
        if realtime.is_empty():
            return
        today = date.today()

        virtual = realtime.select([
            pl.lit(today).alias("date"),
            pl.col("code"),
            pl.col("price").alias("close"),
            pl.col("open"),
            pl.col("high"),
            pl.col("low"),
            pl.col("volume"),
        ])

        cached = self.daily_data.filter(pl.col("date") < today)
        combined = pl.concat([cached, virtual]).sort(["code", "date"])
        new_signal_df = self._compute_signals(combined)

        new_signals = {}
        for r in new_signal_df.iter_rows():
            new_signals[r[0]] = r[1]

        now_ts = time.time()
        alerts = []
        for code, sig in new_signals.items():
            old = self.last_signals.get(code, 0)
            if sig != old:
                last = self.last_alert_time.get(code, 0)
                if now_ts - last > self.alert_cooldown:
                    alerts.append({
                        "code": code,
                        "direction": "买入" if sig == 1 else "卖出",
                    })
                    self.last_alert_time[code] = now_ts

        self.last_signals = new_signals

        if alerts:
            name_map = {
                r["code"]: r.get("name", r["code"])
                for r in realtime.iter_rows()
            }
            buy = [
                {"code": a["code"], "name": name_map.get(a["code"], a["code"])}
                for a in alerts if a["direction"] == "买入"
            ]
            sell = [
                {"code": a["code"], "name": name_map.get(a["code"], a["code"])}
                for a in alerts if a["direction"] == "卖出"
            ]
            print(f"[Monitor] 信号变化: {len(buy)} 买入, {len(sell)} 卖出")
            if notifier:
                notifier.send_signal(buy, sell, str(today))
        return alerts
