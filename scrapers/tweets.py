import logging
import time
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
    def __init__(self, db, ticker):
        self.db = db
        self.session = Session()
        self.ticker = ticker
        self.extracted = 0
        self.started = 0

        with Using(db, [Tweet, TweetsContext]):
            db.create_tables([Tweet, TweetsContext], safe=True)

            self.context, _ = TweetsContext.get_or_create(ticker=ticker,
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
            'q': self.ticker.fullname + ' lang:en',
            'src': 'typd',
            'max_position': self.context.max_position
        }

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)',
            'X-Requested-With': "XMLHttpRequest"
        }

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
        speed = int(self.extracted / spent)

        logging.info('Extracted {} (+{}, {}t/h) up to {}'.format(self.extracted, extracted, speed,
                                                                 datetime.fromtimestamp(oldest)))

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
