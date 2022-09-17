import random

import datetime as dt
import time as _time
from typing import Tuple, Dict, Union

import numpy as np
import pandas as pd
import requests
from opensignals.data.provider import Provider, SIGNALS_TICKER_MAP  
from opensignals import utils


class Yahoo(Provider):
    """Implementation of a stock data price provider that uses the Yahoo! Finance API"""
    def __init(self, ticker_map: pd.DataFrame):
         if ticker_map:
            self.ticker_map =ticker_map
         else:
            self.ticker_map = pd.read_csv(SIGNALS_TICKER_MAP)


    def get_tickers(self) -> pd.DataFrame:
        return Provider.va(self.ticker_map)

    def download_ticker(self, ticker: str, start: dt.datetime, end: dt.datetime) -> Tuple[str, pd.DataFrame]:
        """dowload data for a given ticker"""

        def empty_df() -> pd.DataFrame:
            return pd.DataFrame(columns=[
                "date", "bloomberg_ticker",
                "open", "high", "low", "close",
                "adj_close", "volume", "currency", "provider"])

        retries = 20
        tries = retries + 1
        backoff = 1
        url = f'https://query2.finance.yahoo.com/v8/finance/chart/{ticker}'
        user_agent = random.choice(utils.USER_AGENTS)
        params: Dict[str, Union[int, str]] = dict(
            period1=int(start.timestamp()),
            period2=int(end.timestamp()),
            interval='1d',
            events='div,splits',
        )
        while tries > 0:
            tries -= 1
            try:
                data = requests.get(
                    url=url,
                    params=params,
                    headers={'User-Agent': user_agent}
                )
                data_json = data.json()
                quotes = data_json["chart"]["result"][0]
                if "timestamp" not in quotes:
                    return ticker, empty_df()

                timestamps = quotes["timestamp"]
                ohlc = quotes["indicators"]["quote"][0]
                volumes = ohlc["volume"]
                opens = ohlc["open"]
                closes = ohlc["close"]
                lows = ohlc["low"]
                highs = ohlc["high"]

                adjclose = closes
                if "adjclose" in quotes["indicators"]:
                    adjclose = quotes["indicators"]["adjclose"][0]["adjclose"]

                df = pd.DataFrame({
                    "date": pd.to_datetime(timestamps, unit="s").normalize(),
                    "bloomberg_ticker": ticker,
                    "open": np.array(opens, dtype='float32'),
                    "high": np.array(highs, dtype='float32'),
                    "low": np.array(lows, dtype='float32'),
                    "close": np.array(closes, dtype='float32'),
                    "adj_close": np.array(adjclose, dtype='float32'),
                    "volume": np.array(volumes, dtype='float32'),
                    "currency": quotes['meta']['currency'],
                    "provider": 'yahoo'
                })

                return ticker, df.drop_duplicates().dropna()

            except Exception:
                _time.sleep(backoff)
                backoff = min(backoff * 2, 30)

        return ticker, empty_df()
