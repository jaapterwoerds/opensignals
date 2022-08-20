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
    def __init__(self, api_token) -> None:
        super().__init__()
        self.api_token=api_token
    
    @staticmethod
    def get_tickers() -> pd.DataFrame:
        ticker_map = pd.read_parquet('/Users/jaapterwoerds/workspaces/jaapterwoerds/opensignals/eodhd-map.parquet').reset_index()
        ticker_map['yahoo']=ticker_map['signals_ticker']
        ticker_map = ticker_map[ticker_map.data_provider != 'yahoo']
        ticker_map = ticker_map.dropna(subset=['yahoo'])
        ticker_map =ticker_map.drop_duplicates(subset=['yahoo'])
        logger.info(f'Number of eligible tickers: {ticker_map.shape[0]}')

        if ticker_map['yahoo'].duplicated().any():
            num = ticker_map["yahoo"].duplicated().values.sum()
            raise Exception(f'Found duplicated {num} yahoo tickers')

        if ticker_map['bloomberg_ticker'].duplicated().any():
            num = ticker_map["bloomberg_ticker"].duplicated().values.sum()
            raise Exception(f'Found duplicated {num} bloomberg_ticker tickers')

        return ticker_map
    
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
