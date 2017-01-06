import logging
import time
import random
from datetime import datetime

from peewee import *
from requests import Session
from pyquery import PyQuery

from .ticker import Ticker, TickerField

class Tweet(Model):
    ticker = TickerField()
    id = IntegerField()
    date = TimestampField(utc=True, index=True)
    user_id = IntegerField()
    text = TextField()
    retweet_count = IntegerField()
    favorite_count = IntegerField()

class TweetsContext(Model):
    ticker = TickerField()
    max_position = TextField()

class Scraper:
    _QUERIES = {
        Ticker.GOOG:    'google',
        Ticker.IBM:     'ibm',
        Ticker.WMT:     'walmart',
        Ticker.GE:      'general electric',
        Ticker.MSFT:    'microsoft'
    }

    _USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.85 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64; rv:50.0) Gecko/20100101 Firefox/50.0'
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
    ]

    random.shuffle(_USER_AGENTS)

    def __init__(self, db, ticker):
        self.db = db
        self.session = Session()
        self.ticker = ticker
        self.request_count = 0
        self.extracted = 0
        self.started = 0

        with Using(db, [Tweet, TweetsContext]):
            db.create_tables([Tweet, TweetsContext], safe=True)

            self.context, _ = TweetsContext.get_or_create(ticker=self.ticker,
                                                          defaults={'max_position': ''})

    def scrape(self):
        self.started = time.time()

        if self.context.max_position:
            logging.info('Starting from %s', self.context.max_position)

        with Using(self.db, [Tweet, TweetsContext], False):
            while True:
                self._step()

    def _step(self):
        json = self._fetch()

        tweets = self._extract_tweets(json['items_html'])
        self.context.max_position = json['min_position']

        with self.db.atomic():
            count = Tweet.insert_many(tweets).execute()
            self.context.save()

    def _fetch(self):
        params = {
            'f': 'realtime',
            'q': self._QUERIES[self.ticker] + ' lang:en',
            'src': 'typd',
            'max_position': self.context.max_position
        }

        headers = {
            'User-Agent': self._USER_AGENTS[self.request_count % len(self._USER_AGENTS)],
            'X-Requested-With': "XMLHttpRequest"
        }

        self.request_count += 1

        r = self.session.get('https://twitter.com/i/search/timeline', params=params, headers=headers)

        return r.json()

    def _extract_tweets(self, html):
        pq = PyQuery(html)
        oldest = 0
        extracted = 0

        for tweet_html in pq('div.js-stream-tweet'):
            tweet = self._extract_tweet(tweet_html)

            oldest = min(oldest or tweet['date'], tweet['date'])
            extracted += 1

            yield tweet

        self.extracted += extracted
        spent = (time.time() - self.started) / 3600

        MESSAGE = 'Extracted {total} (+{last}, {speed}t/h) up to {oldest} in {requests} requests'
        logging.info(MESSAGE.format(total=self.extracted,
                                    last=extracted,
                                    speed=int(self.extracted / spent),
                                    oldest=datetime.fromtimestamp(oldest),
                                    requests=self.request_count))

    def _extract_tweet(self, html):
        pq = PyQuery(html)

        return {
            'ticker': self.ticker,
            'id': pq.attr('data-tweet-id'),
            'date': int(pq('small.time span.js-short-timestamp').attr('data-time')),
            'user_id': int(pq('a.js-user-profile-link').attr('data-user-id')),
            'text': pq('p.js-tweet-text').text().replace('# ', '#').replace('@ ', '@'),
            'retweet_count': int(pq('span.ProfileTweet-action--retweet span.ProfileTweet-actionCount')
                                 .attr('data-tweet-stat-count').replace(',', '')),
            'favorite_count': int(pq('span.ProfileTweet-action--favorite span.ProfileTweet-actionCount')
                                  .attr('data-tweet-stat-count').replace(',', ''))
        }
