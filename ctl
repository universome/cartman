#!/usr/bin/env python3

import sys
import logging
import traceback

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-10s %(levelname)-8s %(message)s',
                    datefmt='%d/%m %H:%M:%S')

def scrape_tweets(ticker_name):
    from scrapers.ticker import Ticker
    from scrapers.tweets import Scraper

    from peewee import SqliteDatabase

    db = SqliteDatabase('cartman.db')
    ticker = Ticker[ticker_name]

    db.connect()
    scraper = Scraper(db, ticker)
    scraper.scrape()
    db.close()

def main():
    if sys.argv[1:3] == ['scrape', 'tweets']:
        scrape_tweets(sys.argv[3])

try:
    main()
except KeyboardInterrupt:
    logging.info('Interrupted')
