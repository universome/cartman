from os import path
import json
import time
import logging
from datetime import datetime
from itertools import islice
from collections import OrderedDict
import time

import requests

from base.database import db
from base.schemas import Quote

class Scraper:
    _INTERVAL_IDS = OrderedDict([(60, 2), (300, 3), (600, 4), (900, 5), (1800, 6), (3600, 7), (86400, 8)])
    _INTERVAL_TO_YEAR = {2: 2016, 3: 2015, 4: 2014, 5: 2013, 6: 2012, 7: 2011, 8: 2010}
    _BASE_URL = 'http://export.finam.ru/{ticker}.csv?market=25&em={ticker_id}&code={ticker}&apply=0&df={day}&mf={month}&yf={year}&p={interval_id}&f=lol&e=.csv&cn={ticker}&dtf=1&tmf=1&MSOR=1&mstimever=1&sep=1&sep2=1&datf=1&at=1'

    _TICKERS = OrderedDict([
        ("MMM", 18090), ("T", 19067), ("ADBE", 20563), ("AA", 17997), ("GOOG", 20590), ("AXP", 18009),
        ("AIG", 19070), ("AMT", 20568), ("AAPL", 20569), ("AMAT", 20570), ("BAC", 18011), ("BA", 18010),
        ("CA", 20576), ("CAT", 18026), ("CVX", 18037), ("CSCO", 20580), ("C", 18023), ("KO", 18076),
        ("GLW", 20582), ("DD", 18039), ("EMC", 20585), ("XOM", 18149), ("FSLR", 20586), ("GE", 18055),
        ("GS", 472568), ("HPQ", 18068), ("HD", 18063), ("IBM", 18069), ("IP", 22141), ("INTC", 19069),
        ("JPM", 18074), ("JNJ", 18073), ("MCD", 18080), ("MRK", 18094), ("MSFT", 19068), ("PFE", 18106),
        ("PG", 18107), ("TRV", 22139), ("UTX", 18134), ("VZ", 18137), ("WMT", 18146), ("DIS", 18041),
        ("WFC", 22138), ("YHOO", 19075), ("YNDX", 81151)
    ])

    def __init__(self):
        db.create_tables([Quote], safe=True)

    def scrape(self):
        for ticker in self._TICKERS.keys():
            for interval in self._INTERVAL_IDS:
                response = self._load_quotes(ticker, interval)

                if 'Дождитесь' in response:
                    time.sleep(3)
                    response = self._load_quotes(ticker, interval)

                if 'слишком большой' in response:
                    print('Skipping {interval} for {ticker}'.format(interval, ticker))
                    continue # Finam does not have quotes for this period

                quotes = self._extract_quotes(response, interval)
                n = 100

                with db.atomic():
                    while True:
                        chunk = list(islice(quotes, n))

                        if chunk:
                            Quote.insert_many(chunk).on_conflict('IGNORE').execute()

                        if len(chunk) < n: break

    def _load_quotes(self, ticker, interval):
        interval_id = self._INTERVAL_IDS[interval]
        ticker_id = self._TICKERS[ticker]

        params = {
            'ticker': ticker,
            'ticker_id': ticker_id,
            'interval_id': interval_id,
            'year': self._INTERVAL_TO_YEAR[interval_id],
            'month': 0,
            'day': 1
        }

        logging.info('Getting {ticker} ({interval_id}) since {year}-{month:02}-{day:02}'.format(**params))
        response = requests.get(self._BASE_URL.format(**params))

        return response.text

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
