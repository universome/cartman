#!/usr/bin/env python3

import sys
import logging
import traceback

from playhouse.sqlite_ext import SqliteExtDatabase

from scrapers import ticker, tweets, quotes, articles

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-10s %(levelname)-8s %(message)s',
                    datefmt='%d/%m %H:%M:%S')

SCRAPERS = {
    'tweets': tweets.Scraper,
    'quotes': quotes.Scraper,
    'articles': articles.Scraper
}

def run_scraper(scraper_name, args):
    db = SqliteExtDatabase('cartman.db')

    db.connect()
    scraper = SCRAPERS[scraper_name](db, *args)
    scraper.scrape()
    db.close()

def main():
    if sys.argv[1] == 'scrape':
        run_scraper(sys.argv[2], sys.argv[3:])

try:
    main()
except KeyboardInterrupt:
    logging.info('Interrupted')
