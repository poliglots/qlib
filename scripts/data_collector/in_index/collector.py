# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
India index constituent collector for NSE indices.

Supported indices:
    NSE: NIFTY50, NIFTY500

Usage:
    # Parse Nifty 50 instruments
    $ python collector.py --index_name NIFTY50 --qlib_dir ~/.qlib/qlib_data/in_data --method parse_instruments

    # Parse Nifty 500 instruments
    $ python collector.py --index_name NIFTY500 --qlib_dir ~/.qlib/qlib_data/in_data --method parse_instruments
"""

import sys
import time
from functools import partial
from pathlib import Path
from typing import List

import fire
import requests
import pandas as pd
from loguru import logger

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))

from data_collector.index import IndexBase
from data_collector.utils import deco_retry, get_calendar_list, get_instruments


NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
}

# NSE archives CSV URLs — no cookie/JS requirement
NSE_INDEX_ARCHIVE_URL = {
    "NIFTY50":  "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv",
    "NIFTY500": "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
}

# NSE index name as used in the NSE API (fallback)
NSE_INDEX_API_NAME = {
    "NIFTY50": "NIFTY 50",
    "NIFTY500": "NIFTY 500",
}


def _format_nse_symbol(symbol: str) -> str:
    """Convert raw NSE symbol to Yahoo Finance format (e.g. 'M&M' → 'M&M.NS').
    Dot→dash conversion for filenames is handled by normalize_symbol."""
    s = symbol.strip().strip("$").strip("*")
    return (s + ".NS").upper()


class NSEIndexBase(IndexBase):
    """Base class for NSE index collectors."""

    INST_PREFIX = ""

    @property
    def bench_start_date(self) -> pd.Timestamp:
        return pd.Timestamp("2000-01-01")

    @property
    def calendar_list(self) -> List[pd.Timestamp]:
        _calendar_list = getattr(self, "_calendar_list", None)
        if _calendar_list is None:
            _calendar_list = list(filter(lambda x: x >= self.bench_start_date, get_calendar_list("IN_ALL")))
            setattr(self, "_calendar_list", _calendar_list)
        return _calendar_list

    def format_datetime(self, inst_df: pd.DataFrame) -> pd.DataFrame:
        if self.freq != "day":
            inst_df[self.END_DATE_FIELD] = inst_df[self.END_DATE_FIELD].apply(
                lambda x: (pd.Timestamp(x) + pd.Timedelta(hours=23, minutes=59)).strftime("%Y-%m-%d %H:%M:%S")
            )
        else:
            inst_df[self.START_DATE_FIELD] = inst_df[self.START_DATE_FIELD].apply(
                lambda x: pd.Timestamp(x).strftime("%Y-%m-%d")
            )
            inst_df[self.END_DATE_FIELD] = inst_df[self.END_DATE_FIELD].apply(
                lambda x: pd.Timestamp(x).strftime("%Y-%m-%d")
            )
        return inst_df

    @property
    def nse_index_api_name(self) -> str:
        raise NotImplementedError("subclass must define nse_index_api_name")

    @deco_retry
    def _fetch_constituents(self) -> pd.DataFrame:
        import io
        index_key = self.index_name.upper()
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        # Primary: NSE archives CSV — no cookie requirement
        archive_url = NSE_INDEX_ARCHIVE_URL.get(index_key)
        if archive_url:
            resp = requests.get(archive_url, headers=headers, timeout=15)
            if resp.status_code == 200 and resp.content:
                df = pd.read_csv(io.StringIO(resp.text))
                if "Symbol" in df.columns and not df.empty:
                    return df.rename(columns={"Symbol": "symbol"})

        # Fallback: NSE API with cookie session
        session = requests.Session()
        session.get("https://www.nseindia.com/market-data/live-equity-market", headers=headers, timeout=15)
        time.sleep(2)
        api_name = NSE_INDEX_API_NAME.get(index_key, index_key)
        url = f"https://www.nseindia.com/api/equity-stockIndices?index={requests.utils.quote(api_name)}"
        resp = session.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            raise ValueError(f"NSE API returned empty data for index: {api_name}")
        return pd.DataFrame(data)

    def get_new_companies(self) -> pd.DataFrame:
        logger.info(f"Fetching current constituents of {self.index_name} from NSE...")
        df = self._fetch_constituents()
        df = df[df["symbol"].notna()].copy()
        df[self.SYMBOL_FIELD_NAME] = df["symbol"].map(_format_nse_symbol)
        df[self.START_DATE_FIELD] = self.bench_start_date
        df[self.END_DATE_FIELD] = self.DEFAULT_END_DATE
        logger.info(f"Got {len(df)} constituents for {self.index_name}.")
        return df.loc[:, self.INSTRUMENTS_COLUMNS]

    def get_changes(self) -> pd.DataFrame:
        # NSE does not publish a machine-readable historical changes feed;
        # returning an empty DataFrame causes parse_instruments to use only
        # the current snapshot (get_new_companies).
        logger.warning(
            f"Historical constituent changes for {self.index_name} are not available via the NSE API. "
            "Only the current snapshot will be used."
        )
        return pd.DataFrame(columns=[self.DATE_FIELD_NAME, self.CHANGE_TYPE_FIELD, self.SYMBOL_FIELD_NAME])


class NIFTY50Index(NSEIndexBase):
    @property
    def nse_index_api_name(self) -> str:
        return NSE_INDEX_API_NAME["NIFTY50"]

    @property
    def bench_start_date(self) -> pd.Timestamp:
        return pd.Timestamp("1996-07-04")  # Nifty 50 inception date


class NIFTY500Index(NSEIndexBase):
    @property
    def nse_index_api_name(self) -> str:
        return NSE_INDEX_API_NAME["NIFTY500"]

    @property
    def bench_start_date(self) -> pd.Timestamp:
        return pd.Timestamp("1999-06-15")  # Nifty 500 inception date


if __name__ == "__main__":
    fire.Fire(partial(get_instruments, market_index="in_index"))
