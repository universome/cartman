import logging
import time
import random
from datetime import datetime, timedelta

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
        'Mozilla/5.0 (X11; Linux x86_64; rv:50.0) Gecko/20100101 Firefox/50.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko'
    ]

    random.shuffle(_USER_AGENTS)

    def __init__(self, db, ticker):
        self.db = db
        self.session = Session()
        self.ticker = ticker
        self.request_count = 0
        self.fail_count = 0
        self.extracted = 0
        self.accepted = 0
        self.startup = 0
        self.upper_date = None
        self.until = None

        with Using(db, [Tweet, TweetsContext]):
            db.create_tables([Tweet, TweetsContext], safe=True)

            self.context, _ = TweetsContext.get_or_create(ticker=self.ticker,
                                                          defaults={'max_position': ''})

    def scrape(self):
        self.startup = time.time()

        if self.context.max_position:
            logging.info('Starting from %s', self.context.max_position)

        with Using(self.db, [Tweet, TweetsContext], False):
            while True:
                self._step()

    def _step(self):
        for i in range(len(self._USER_AGENTS), 0, -1):
            json, ua = self._fetch()
            self.request_count += 1

            html = json['items_html'].strip()
            if html:
                break

            self.fail_count += 1

            logging.warning('Empty document for %s', ua)

            if i > 1:
                logging.info('Retrying...')
            else:
                logging.info('Degradation...')
                self._degrade()
                return

        tweets = list(self._extract_tweets(html))
        self.context.max_position = json['min_position']

        if tweets:
            with self.db.atomic():
                count = Tweet.insert_many(tweets).execute()
                self.context.save()

    def _fetch(self):
        until = (' until:' + self.until if self.until else '')

        params = {
            'f': 'realtime',
            'q': self._QUERIES[self.ticker] + ' lang:en' + until,
            'src': 'typd',
            'max_position': self.context.max_position
        }

        ua = self._USER_AGENTS[self.request_count % len(self._USER_AGENTS)]

        headers = {
            'User-Agent': ua,
            'X-Requested-With': "XMLHttpRequest"
        }

        r = self.session.get('https://twitter.com/i/search/timeline', params=params, headers=headers)

        return r.json(), ua

    def _extract_tweets(self, html):
        pq = PyQuery(html)
        oldest = 0
        extracted = 0
        accepted = 0

        for tweet_html in pq('div.js-stream-tweet'):
            tweet = self._extract_tweet(tweet_html)

            oldest = min(oldest or tweet['date'], tweet['date'])
            extracted += 1

            if self._filter(tweet):
                accepted += 1
                yield tweet

        self.extracted += extracted
        self.accepted += accepted
        spent = (time.time() - self.startup) / 3600

        logging.info('E: {} +{:2} {:5}/h    A: {} +{:2} {:5}/h    O: {}    R: {}    F: {}'.format(
            self.extracted, extracted, int(self.extracted / spent),
            self.accepted, accepted, int(self.accepted / spent),
            datetime.fromtimestamp(oldest), self.request_count, self.fail_count
        ))

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

    def _filter(self, tweet):
        if tweet['retweet_count'] == 0 and tweet['favorite_count'] == 0:
            return False

        return not self.upper_date or tweet['date'] < self.upper_date

    def _degrade(self):
        self.context.max_position = ''

        oldest = (Tweet
            .select()
            .where(Tweet.ticker == self.ticker)
            .order_by(Tweet.date)
            .get())

        self.upper_date = oldest.date.timestamp()
        self.until = (oldest.date + timedelta(days=1)).strftime('%Y-%m-%d')
