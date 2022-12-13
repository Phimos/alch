import re
from typing import List, Optional

import numpy as np
import pandas as pd
import tushare
from retry import retry
from tushare.pro.client import DataApi

from ...utils.time import split_date_range
from .base import BaseDataSource


class TushareDataSource(BaseDataSource):
    _api: DataApi

    def __init__(self, token: str) -> None:
        self._api = tushare.pro_api(token)

    @classmethod
    def convert_symbol(cls, symbol: str) -> str:
        if symbol.startswith("6"):
            return "{}.SH".format(symbol)
        else:
            return "{}.SZ".format(symbol)

    @classmethod
    def convert_date(cls, date: pd.Timestamp) -> str:
        return date.strftime("%Y%m%d")

    def fetch(
        self,
        dataset: str,
        *,
        symbol: Optional[str] = None,
        start_date: Optional[pd.Timestamp] = None,
        end_date: Optional[pd.Timestamp] = None,
        trade_date: Optional[pd.Timestamp] = None,
        frequency: Optional[str] = None,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        kwargs = {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "trade_date": trade_date,
            "frequency": frequency,
            "fields": fields,
        }

        match dataset:
            case "history" | "ohlcv":
                return self.fetch_historical_data(**kwargs)
            case "adjust_factor":
                return self.fetch_adjust_factor(**kwargs)
            case _:
                raise ValueError(f"unsupported dataset: {dataset}")

    @retry(tries=5, delay=10)
    def fetch_historical_data_daily(
        self,
        symbol: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        trade_date: Optional[pd.Timestamp] = None,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        data: pd.DataFrame = self._api.daily(
            ts_code=self.convert_symbol(symbol),
            start_date=self.convert_date(start_date),
            end_date=self.convert_date(end_date),
        )
        data.sort_values(by="trade_date", inplace=True)
        data.reset_index(drop=True, inplace=True)

        return pd.DataFrame(
            {
                "symbol": data["ts_code"].str.slice(0, 6),
                "date": pd.to_datetime(data["trade_date"], format="%Y%m%d"),
                "open": data["open"].astype(np.float64),
                "high": data["high"].astype(np.float64),
                "low": data["low"].astype(np.float64),
                "close": data["close"].astype(np.float64),
                "volume": data["vol"].astype(np.float64) * 100.0,
                "amount": data["amount"].astype(np.float64) * 1000.0,
                "prev": data["pre_close"].astype(np.float64),
            }
        )

    @retry(tries=5, delay=10)
    def fetch_historical_data_high_freq(
        self,
        symbol: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        trade_date: Optional[pd.Timestamp] = None,
        frequency: Optional[str] = None,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        data: pd.DataFrame = tushare.pro_bar(
            api=self._api,
            ts_code=self.convert_symbol(symbol),
            start_date=start_date.strftime("%Y-%m-%d 09:00:00"),
            end_date=end_date.strftime("%Y-%m-%d 17:00:00"),
            freq=frequency,
        )
        data.sort_values(by="trade_time", inplace=True)
        data.reset_index(drop=True, inplace=True)

        return pd.DataFrame(
            {
                "symbol": data["ts_code"].str.slice(0, 6),
                "date": pd.to_datetime(data["trade_date"]),
                "time": pd.to_datetime(data["trade_time"]),
                "open": data["open"].astype(np.float64),
                "high": data["high"].astype(np.float64),
                "low": data["low"].astype(np.float64),
                "close": data["close"].astype(np.float64),
                "volume": data["vol"].astype(np.float64),
                "amount": data["amount"].astype(np.float64),
                "prev": data["pre_close"].astype(np.float64),
            }
        )

    def fetch_historical_data(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[pd.Timestamp] = None,
        end_date: Optional[pd.Timestamp] = None,
        trade_date: Optional[pd.Timestamp] = None,
        frequency: Optional[str] = None,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        assert symbol is not None
        assert frequency is not None
        assert fields is None
        assert trade_date is None
        start_date = pd.Timestamp("2010-01-01") if start_date is None else start_date
        end_date = pd.Timestamp.now().floor("D") if end_date is None else end_date

        match frequency:
            case "daily":
                return self.fetch_historical_data_daily(symbol, start_date, end_date)
            case "1min" | "5min" | "15min" | "30min" | "60min":
                num_minutes = int(re.match(r"(\d+)min", frequency).group(1))
                max_days_per_iteration = 10000 // (240 // num_minutes)
                return pd.concat(
                    [
                        self.fetch_historical_data_high_freq(
                            symbol, start, end, frequency=frequency
                        )
                        for start, end in split_date_range(
                            start_date=start_date,
                            end_date=end_date,
                            period=pd.Timedelta(days=max_days_per_iteration),
                        )
                    ]
                )
            case _:
                raise ValueError(f"Unsupported frequency: {frequency}")

    @retry(tries=5, delay=10)
    def fetch_adjust_factor(
        self,
        symbol: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        trade_date: Optional[pd.Timestamp] = None,
        frequency: Optional[str] = None,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        data: pd.DataFrame = self._api.adj_factor(
            ts_code=self.convert_symbol(symbol),
            start_date=self.convert_date(start_date),
            end_date=self.convert_date(end_date),
        )
        data.sort_values(by="trade_date", inplace=True)
        data.reset_index(drop=True, inplace=True)

        return pd.DataFrame(
            {
                "symbol": data["ts_code"].str.slice(0, 6),
                "date": pd.to_datetime(data["trade_date"], format="%Y%m%d"),
                "adj_factor": data["adj_factor"].astype(np.float64),
            }
        )
