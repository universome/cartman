import json
import time
import logging
import os
from os import path
from datetime import datetime
from itertools import islice, chain

import requests
from peewee import *
from pyquery import PyQuery as pq

from base.database import db, BaseModel

class Article(BaseModel):
    oid = IntegerField(primary_key=True)
    ticker = CharField(5)
    date = TimestampField(utc=True, index=True)
    title = TextField()
    seo_title = TextField(null=True)
    url = TextField()
    category = TextField(null=True)
    word_count = IntegerField(null=True)
    section = TextField(null=True)
    type_of_material = TextField(null=True)
    first_paragraph = TextField(null=True)
    keywords = TextField(null=True)
    has_multimedia = BooleanField()

class Scraper:
    _API_KEY = os.environ['NY_API_KEY']
    _URL = 'https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key=' + _API_KEY
    _START_YEAR = 2011
    _FINISH_YEAR = 2017
    _FINISH_MONTH = 2

    _COMPANIES_TO_TICKERS = {
        'Google': 'GOOG',
        'International Business Machines': 'IBM',
        'IBM': 'IBM',
        'Walmart': 'WMT',
        'General Electric': 'GE',
        'Microsoft': 'MSFT'
    }

    def __init__(self):
        db.create_tables([Article], safe=True)

    def scrape(self):
        articles = self._extract_articles()
        n = 50
        count = 0

        while True:
            with db.atomic():
                chunk = list(islice(articles, n))
                count += len(chunk)
                logging.info('Saved {} articles'.format(count))

                if len(chunk) != 0: Article.insert_many(chunk).execute()
                if len(chunk) < n: break

    def _extract_articles(self):
        count = 0
        for article in chain.from_iterable(self._extract_archives()):
            for keyword in article['keywords']:
                for company, ticker in self._COMPANIES_TO_TICKERS.items():
                    if not company in keyword['value']: continue

                    # Sometimes we have date in an other format :|
                    date_str = article['pub_date'].replace('Z', '+0000')

                    yield {
                        'ticker': ticker,
                        'date': time.mktime(datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S+%f').timetuple()),
                        'title': article['headline']['main'],
                        'seo_title': article['seo_headline'] if 'seo_headline' in article else None,
                        'url': article['web_url'],
                        'category': article['news_desk'] if 'news_desk' in article else None,
                        'word_count': int(article['word_count']) if article['word_count'] != None else None,
                        'section': article['section_name'] if 'section_name' in article else None,
                        'type_of_material': article['type_of_material'] if 'type_of_material' in article else None,
                        'first_paragraph': article['lead_paragraph'] if 'lead_paragraph' in article else None,
                        'keywords': ','.join(map(lambda k: k['value'], article['keywords'])),
                        'has_multimedia': len(article['multimedia']) > 1 if 'multimedia' in article else None
                    }
            count += 1
            if count % 10000 == 0: logging.info('Processed {} articles'.format(count))

    def _extract_archives(self):
        params = {'year': self._START_YEAR, 'month': 1}

        while params['year'] < self._FINISH_YEAR or params['month'] < self._FINISH_MONTH:
            archive = self._extract_archive(params)

            if 'docs' in archive:
                yield archive['docs']
            else:
                logging.error('Could not extract archive {month:02}/{year}'.format(**params))

            if params['month'] < 12:
                params['month'] += 1
            else:
                params['year'] += 1
                params['month'] = 1

    def _extract_archive(self, params):
        logging.info('Extracting archive {month:02}/{year}'.format(**params))

        response = requests.get(self._URL.format(**params))

        logging.info('Loading archive: {}'.format(response.url))

        try:
            archive = response.json()['response']
        except ValueError:
            logging.error('Error occured when decoding response: {}'.format(response.text))
            archive = []

        return archive
