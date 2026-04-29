import akshare as ak
import polars as pl
from datetime import date
from pathlib import Path
from src.network import HEADERS


class DataFetcher:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self._use_akshare = None

    def _akshare_works(self):
        if self._use_akshare is not None:
            return self._use_akshare
        try:
            ak.stock_zh_a_spot_em()
            self._use_akshare = True
        except Exception:
            try:
                ak.stock_info_a_code_name()
                self._use_akshare = True
            except Exception:
                self._use_akshare = False
        return self._use_akshare

    def _baostock_login(self):
        try:
            import baostock as bs
            bs.login()
            return bs, True
        except Exception:
            return None, False

    def _prefix(self, code):
        if code.startswith(("5", "6", "9")):
            return "sh."
        return "sz."

    def fetch_stock_list(self):
        csv_path = Path("stock_list.csv")
        if csv_path.exists():
            df = pl.read_csv(csv_path, schema_overrides={"代码": pl.Utf8})
            if not df.is_empty():
                return df

        if self._akshare_works():
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

        bs, ok = self._baostock_login()
        if ok:
            try:
                rs = bs.query_stock_basic()
                data = rs.get_data()
                bs.logout()
                stocks = data[data["type"] == "1"]
                return pl.from_pandas(stocks[["code", "code_name"]]).rename(
                    {"code": "代码", "code_name": "名称"}
                ).with_columns(
                    pl.col("代码").str.replace(r"^sh\.|^sz\.", "")
                )
            except Exception:
                try:
                    bs.logout()
                except Exception:
                    pass

        raise RuntimeError("无法获取股票列表，请检查网络连接")

    def fetch_daily(self, code, name, start_date, end_date):
        if self._akshare_works():
            data = self._fetch_akshare(code, name, start_date, end_date)
            if data is not None:
                return data
        return self._fetch_baostock(code, name, start_date, end_date)

    def _fetch_akshare(self, code, name, start_date, end_date):
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
            "日期": "date", "开盘": "open", "收盘": "close",
            "最高": "high", "最低": "low", "成交量": "volume",
        }).select(["date", "code", "name", "open", "close", "high", "low", "volume"])
        return data

    def _fetch_baostock(self, code, name, start_date, end_date):
        try:
            import baostock as bs
        except ImportError:
            return None
        try:
            bs.login()
            symbol = self._prefix(code) + code
            rs = bs.query_history_k_data_plus(
                symbol,
                "date,code,open,high,low,close,volume",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="2",
            )
            if rs.error_code != "0":
                bs.logout()
                return None
            import pandas as pd
            rows = [rs.get_data()]
            while rs.next():
                chunk = rs.get_data()
                if chunk.empty:
                    break
                rows.append(chunk)
            bs.logout()
            if not rows or rows[0].empty:
                return None
            merged = pd.concat(rows, ignore_index=True)
        except Exception:
            try:
                bs.logout()
            except Exception:
                pass
            return None

        df = pl.from_pandas(merged)
        if df.is_empty():
            return None
        return df.with_columns([
            pl.lit(code).alias("code"),
            pl.lit(name).alias("name"),
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"),
            pl.col("open").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
        ]).select(["date", "code", "name", "open", "close", "high", "low", "volume"])

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
