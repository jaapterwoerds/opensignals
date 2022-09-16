from distutils.command.install_egg_info import to_filename
import random

import datetime as dt
from re import A
import time as _time
from typing import Tuple, Dict, Union

import logging
import numpy as np
import pandas as pd
import requests
from opensignals.data.provider import Provider
from opensignals import utils

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)
class EodHisotricalData(Provider):
    """Implementation of a stock data price provider that uses the eodhistoricaldata.com API  https://eodhistoricaldata.com/financial-apis/ """
    def __init__(self, api_token, ticker_map:pd) -> None:
        super().__init__()
        self.api_token=api_token
        self.ticker_map = ticker_map
    
    def get_tickers(self) -> pd.DataFrame:
        ticker_map = self.ticker_map
        ticker_map = ticker_map[ticker_map.data_provider != 'eodhd']
        ticker_map['yahoo']=ticker_map['signals_ticker']
        return Provider.get_tickers(ticker_map)
    
    def download_ticker(self, ticker: str, start: dt.datetime, end: dt.datetime) -> Tuple[str, pd.DataFrame]:
        """dowload data for a given ticker"""

        def empty_df() -> pd.DataFrame:
            return pd.DataFrame(columns=[
                "date", "bloomberg_ticker",
                "open", "high", "low", "close",
                "adj_close", "volume", "currency", "provider"])

        retries = 2
        tries = retries + 1
        backoff = 1
        url = f"https://eodhistoricaldata.com/api/eod/{ticker}"
        params: Dict[str, Union[int, str]] =dict({
            'from': start.strftime('%Y-%m-%d'),
            'to': end.strftime('%Y-%m-%d'),
            'fmt':'json',
            'api_token': self.api_token
        })
      
        while tries > 0:
            tries -= 1
            try:
                data = requests.get(
                    url=url,
                    params=params
                    
                )
                if not data.ok:
                    break
                else:
                    quotes = pd.read_json(data.content.decode('utf-8'),dtype={
                        'date': np.datetime64,
                        'open': np.float32,
                        'close': np.float32,
                        'high': np.float32,
                        'low': np.float32,
                        'volume': np.float32,
                        'adjusted_close': np.float32
                    },orient='records')
                    quotes = quotes.rename({'adjusted_close': 'adj_close'})
                    quotes['provider']= 'eodhistoricaldata'
                    quotes['currency'] = 'unknown'
                    quotes['bloomberg_ticker'] = ticker
                    return ticker, quotes.drop_duplicates().dropna()

            except Exception as e:
                logger.exception(e)
                logger.debug('Exception trying to download %s:  %s',ticker, e)
                _time.sleep(backoff)
                backoff = min(backoff * 2, 30)
               
        return ticker, empty_df()
