import os
import logging
from datetime import datetime
from itertools import tee, islice

from peewee import *
from requests import Session

from base.database import db
from base.schemas import Tweet, News, TweetPolarity, NewsPolarity

class Extractor:
    _APP_ID = os.environ['S140_APP_ID']
    _CHUNK_SIZE = 10000

    def __init__(self, type, start=None, end=None):
        assert type in ['tweets', 'news']

        self.type = type
        self.name = 'tweet' if type == 'tweets' else 'news'
        self.model = TweetPolarity if type == 'tweets' else NewsPolarity
        self.start = datetime.strptime(start, '%Y-%m-%d') if start else datetime.utcfromtimestamp(0)
        self.end = datetime.strptime(end, '%Y-%m-%d') if end else datetime.utcnow()
        self.session = Session()

        db.create_tables([self.model], safe=True);

    def extract(self):
        logging.info('Extracting polarity for {} from {} to {}'.format(self.type, self.start, self.end))

        if self.type == 'tweets':
            query = (Tweet
                     .select(Tweet.oid, Tweet.text)
                     .where((self.start < Tweet.date) & (Tweet.date < self.end)))
        elif self.type == 'news':
            query = (News
                     .select(News.oid,
                             News.title.concat(' ').concat(fn.COALESCE(News.description, '')).alias('text'))
                     .where((self.start < News.date) & (News.date < self.end)))

        self._handle(query.dicts().iterator())

    def _handle(self, it):
        count = 0

        while True:
            chunk = list(islice(it, self._CHUNK_SIZE))

            if not chunk:
                break

            data = self._fetch(chunk)

            # XXX(loyd): we have some troubles with transactions within peewee.
            self._save(data)

            count += len(data)

            logging.info('Extracted {} (+{}) {}'.format(count, len(data), self.type))

    def _fetch(self, chunk):
        r = self.session.post('http://www.sentiment140.com/api/bulkClassifyJson',
                         params={'appid': self._APP_ID},
                         json={'data': chunk})

        data = r.json()['data']

        return [{self.name: d['oid'], 'polarity': d['polarity'] // 2 - 1} for d in data]

    def _save(self, data):
        for i in range(0, len(data), 100):
            self.model.insert_many(data[i:i+100]).execute()
