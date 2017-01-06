from os import path
import json
import time
import logging
from datetime import datetime

import requests
from peewee import *

TICKERS = ['MMM', 'T', 'ADBE', 'AA', 'GOOG', 'AXP', 'AIG', 'AMT', 'AAPL', 'AMAT', 'BAC', 'BA', 'CA', 'CAT', 'CVX', 'CSCO', 'C', 'KO', 'GLW', 'DD', 'EMC', 'XOM', 'FSLR', 'GE', 'HPQ', 'HD', 'IBM', 'IP', 'INTC', 'JPM', 'JNJ', 'MCD', 'MRK', 'MSFT', 'PFE', 'PG', 'TRV', 'UTX', 'VZ', 'WMT', 'DIS', 'WFC', 'YHOO', 'YNDX']
INTERVAL_IDS = {60: 2, 300: 3, 600: 4, 900: 5, 1800: 6, 3600: 7}
INTERVAL_TO_YEAR = {2: 2016, 3: 2015, 4: 2014, 5: 2013, 6: 2012, 7: 2011}
BASE_URL = 'http://export.finam.ru/{ticker}.csv?market=25&em=20590&code={ticker}&apply=0&df={day}&mf={month}&yf={year}&p={interval_id}&f=lol&e=.csv&cn={ticker}&dtf=1&tmf=1&MSOR=1&mstimever=1&sep=1&sep2=1&datf=1&at=1'

class Quote(Model):
    ticker = CharField()
    date = TimestampField()
    interval = DecimalField()
    open_price = DecimalField()
    low_price = DecimalField()
    high_price = DecimalField()
    close_price = DecimalField()
    volume = DecimalField()

class Scraper:
    def __init__(self, db):
        self.db = db

        with Using(db, [Quote]):
            db.create_tables([Quote], safe=True)

    def scrape(self):
        for ticker in TICKERS:
            for interval in INTERVAL_IDS:
                raw_quotes = self._load_quotes(ticker, interval)
                self._transfer_quotes_to_db(raw_quotes)

    def _load_quotes(self, ticker, interval):
        params = {
            'ticker': ticker,
            'interval_id': INTERVAL_IDS[interval],
            'year': INTERVAL_TO_YEAR[INTERVAL_IDS[interval]],
            'month': 0,
            'day': 1
        }

        logging.info("Getting {ticker} ({interval_id}) since {day}.{month}.{year}".format(**params))
        quotes = requests.get(BASE_URL.format(**params)).text

        return quotes

    def _transfer_quotes_to_db(self, quotes):
        for quote in quotes.splitlines()[1:]:
            with Using(self.db, [Quote], False):
                with self.db.atomic():
                    quote = quote.split(',')

                    Quote.insert(**{
                        'ticker': quote[0],
                        'date': time.mktime(datetime.strptime(quote[2] + quote[3], '%Y%m%d%H%M%S').timetuple()),
                        'interval': int(quote[1]) * 60,
                        'open_price': float(quote[4]),
                        'high_price': float(quote[5]),
                        'low_price': float(quote[6]),
                        'close_price': float(quote[7]),
                        'volume': int(quote[8])
                    }).execute()
