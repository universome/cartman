#!/usr/bin/env python3

import sys
import logging
import traceback
import os
from os import path

from dotenv import load_dotenv
load_dotenv(path.join(path.dirname(__file__), '.env'))

from scraping import tweets, quotes, articles, news
from extraction import polarity

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-10s %(levelname)-8s %(message)s',
                    datefmt='%d/%m %H:%M:%S')

SCRAPERS = {
    'tweets': tweets.Scraper,
    'quotes': quotes.Scraper,
    'articles': articles.Scraper,
    'news': news.Scraper
}

def run_scraper(name, args):
    scraper = SCRAPERS[name](*args)
    scraper.scrape()

EXTRACTORS = {
    'polarity': polarity.Extractor
}

def run_extractor(name, args):
    extractor = EXTRACTORS[name](*args)
    extractor.extract()

def main():
    if sys.argv[1] == 'scrape':
        run_scraper(sys.argv[2], sys.argv[3:])
    elif sys.argv[1] == 'extract':
        run_extractor(sys.argv[2], sys.argv[3:])

try:
    main()
except KeyboardInterrupt:
    logging.info('Interrupted')
