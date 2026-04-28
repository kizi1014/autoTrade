import akshare as ak
import polars as pl
from datetime import date
from src.network import HEADERS


class DataFetcher:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage

    def fetch_stock_list(self):
        for api_func, (code_col, name_col) in [
            (ak.stock_zh_a_spot_em, ("代码", "名称")),
            (ak.stock_info_a_code_name, ("证券代码", "证券简称")),
        ]:
            try:
                df = api_func()
                return pl.from_pandas(df[[code_col, name_col]]).rename(
                    {code_col: "代码", name_col: "名称"}
                )
            except Exception:
                continue
        raise RuntimeError("无法获取股票列表，请检查网络连接")

    def fetch_daily(self, code, name, start_date, end_date):
        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", "") if end_date else "",
                adjust=self.config["data"]["adjust"],
            )
        except Exception:
            return None
        if df is None or df.empty:
            return None
        data = pl.from_pandas(df)
        data = data.with_columns([
            pl.lit(code).alias("code"),
            pl.lit(name).alias("name"),
            pl.col("日期").str.strptime(pl.Date, "%Y-%m-%d"),
            pl.col("开盘").cast(pl.Float64),
            pl.col("收盘").cast(pl.Float64),
            pl.col("最高").cast(pl.Float64),
            pl.col("最低").cast(pl.Float64),
            pl.col("成交量").cast(pl.Float64),
        ]).rename({
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
        }).select(["date", "code", "name", "open", "close", "high", "low", "volume"])
        return data

    def update_all(self):
        cfg = self.config["data"]
        stocks = self.fetch_stock_list()
        codes = cfg.get("stock_pool") or stocks["代码"].to_list()
        names = dict(stocks.iter_rows())

        start = cfg.get("start_date", "2015-01-01")
        end = cfg.get("end_date") or date.today().strftime("%Y-%m-%d")

        from rich.progress import track
        for code in track(codes, description="[cyan]正在下载数据..."):
            name = names.get(code, code)
            try:
                data = self.fetch_daily(code, name, start, end)
                if data is not None:
                    existing = self.storage.load(code)
                    if existing is not None and not existing.is_empty():
                        latest = existing["date"].max()
                        data = data.filter(pl.col("date") > latest)
                        if not data.is_empty():
                            combined = pl.concat([existing, data]).unique(
                                subset=["date", "code"], keep="first"
                            ).sort(["date"])
                            self.storage.save(code, combined)
                    else:
                        self.storage.save(code, data)
            except Exception:
                pass
