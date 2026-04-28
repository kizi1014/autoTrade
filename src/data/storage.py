import polars as pl
from pathlib import Path


class DataStorage:
    def __init__(self, config):
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)

    def save(self, code, data):
        path = self.data_dir / f"{code}.parquet"
        data.write_parquet(path)

    def load(self, code):
        path = self.data_dir / f"{code}.parquet"
        if path.exists():
            return pl.read_parquet(path)
        return None

    def load_all(self):
        dfs = []
        for f in sorted(self.data_dir.glob("*.parquet")):
            dfs.append(pl.read_parquet(f))
        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs)

    def list_stocks(self):
        return [f.stem for f in self.data_dir.glob("*.parquet")]

    def list_stocks_with_names(self):
        results = []
        for f in self.data_dir.glob("*.parquet"):
            df = pl.read_parquet(f, n_rows=1)
            name = df["name"][0] if "name" in df.columns else f.stem
            results.append((f.stem, name))
        return results

    def get_latest_date(self, code):
        path = self.data_dir / f"{code}.parquet"
        if path.exists():
            df = pl.read_parquet(path)
            return df["date"].max()
        return None
