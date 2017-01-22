from os import path
import json
import time
import logging
from datetime import datetime
from itertools import islice

import requests

from base.database import db
from base.schemas import Quote

class Scraper:
    _INTERVAL_IDS = {60: 2, 300: 3, 600: 4, 900: 5, 1800: 6, 3600: 7}
    _INTERVAL_TO_YEAR = {2: 2016, 3: 2015, 4: 2014, 5: 2013, 6: 2012, 7: 2011}
    _BASE_URL = 'http://export.finam.ru/{ticker}.csv?market=25&em=20590&code={ticker}&apply=0&df={day}&mf={month}&yf={year}&p={interval_id}&f=lol&e=.csv&cn={ticker}&dtf=1&tmf=1&MSOR=1&mstimever=1&sep=1&sep2=1&datf=1&at=1'

    _TICKERS = [
        'AA', 'AAPL', 'ADBE', 'AIG', 'AMAT', 'AMT', 'AXP', 'BA', 'BAC', 'C', 'CA', 'CAT', 'CSCO',
        'CVX', 'DD', 'DIS', 'EMC', 'FSLR', 'GE', 'GLW', 'GOOG', 'HD', 'HPQ', 'IBM', 'INTC', 'IP',
        'JNJ', 'JPM', 'KO', 'MCD', 'MMM', 'MRK', 'MSFT', 'PFE', 'PG', 'T', 'TRV', 'UTX', 'VZ',
        'WFC', 'WMT', 'XOM', 'YHOO', 'YND'
    ]

    def __init__(self):
        db.create_tables([Quote], safe=True)

    def scrape(self):
        for ticker in self._TICKERS:
            for interval in self._INTERVAL_IDS:
                raw_quotes = self._load_quotes(ticker, interval)
                quotes = self._extract_quotes(raw_quotes, interval)
                n = 100

                with db.atomic():
                    while True:
                        chunk = list(islice(quotes, n))

                        if chunk:
                            Quote.insert_many(chunk).on_conflict('IGNORE').execute()

                        if len(chunk) < n: break

    def _load_quotes(self, ticker, interval):
        interval_id = self._INTERVAL_IDS[interval]

        params = {
            'ticker': ticker,
            'interval_id': interval_id,
            'year': self._INTERVAL_TO_YEAR[interval_id],
            'month': 0,
            'day': 1
        }

        logging.info('Getting {ticker} ({interval_id}) since {year}-{month:02}-{day:02}'.format(**params))
        quotes = requests.get(self._BASE_URL.format(**params)).text

        return quotes

    def _extract_quotes(self, quotes, interval):
        for quote in quotes.splitlines()[1:]:
            quote = quote.split(',')

            yield {
                'ticker': quote[0],
                'date': time.mktime(datetime.strptime(quote[2] + quote[3], '%Y%m%d%H%M%S').timetuple()),
                'interval': interval,
                'open_price': float(quote[4]),
                'high_price': float(quote[5]),
                'low_price': float(quote[6]),
                'close_price': float(quote[7]),
                'volume': int(quote[8])
            }
