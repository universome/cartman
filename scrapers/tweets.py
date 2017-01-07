import re
import logging
import time
import random
from datetime import datetime, timedelta, timezone
from collections import deque

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

    class Meta:
        primary_key = CompositeKey('ticker', 'id')
        without_rowid = True

class Scraper:
    _QUERIES = {
        Ticker.IBM:     'ibm',
        Ticker.WMT:     'walmart',
        Ticker.GE:      'general electric',
        Ticker.MSFT:    'microsoft',
        Ticker.ADBE:    'adobe',
        Ticker.YHOO:    'yahoo'
    }

    _USER_AGENTS = [
        '',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
    ]

    random.shuffle(_USER_AGENTS)

    def __init__(self, db, ticker, until=None):
        self.db = db
        self.session = Session()
        self.ticker = Ticker[ticker]
        self.query = self._QUERIES[self.ticker]
        self.max_position = ''

        # Statistics.
        self.time_mark = 0
        self.skip_count = 0
        self.request_count = 0
        self.fail_count = 0
        self.extracted_count = 0
        self.accepted_count = 0
        self.recent_stats = deque()

        with Using(db, [Tweet]):
            db.create_tables([Tweet], safe=True)

            if until:
                self.until = datetime.strptime(until, '%Y-%m-%d')
            else:
                self.until = self._get_oldest_date()

    def scrape(self):
        self.startup = time.time()
        self.time_mark = time.time()

        logging.info('Starting at %s', self.until)

        with Using(self.db, [Tweet], False):
            while self._step():
                pass

    def _step(self):
        max_attempts = len(self._USER_AGENTS)

        for attempt in range(max_attempts + 1):
            if attempt == max_attempts:
                self.until -= timedelta(days=1)
                logging.info('Skipping to {}...'.format(self.until))
                self.skip_count += 1
            elif attempt > 0:
                logging.info('Sleeping and retrying again...')
                time.sleep(2)

            response, user_agent = self._fetch()
            self.request_count += 1

            if 'items_html' in response and 'min_position' in response:
                html = response['items_html'].strip()

                if html and len(response['min_position']) > 16:
                    tweet_it = self._extract_tweets(html)
                    tweets = None

                    try:
                        tweets = self._process_tweets(tweet_it)
                    except Exception as ex:
                        logging.warning('Error while parsing: {}'.format(ex))

                    if tweets is not None:
                        break

            self.fail_count += 1

            response = str(response)
            if len(response) > 1000:
                response = response[:1000] + ' [..]'

            logging.warning('Step failed')
            logging.warning('  User-Agent: {}'.format(user_agent))
            logging.warning('  Response: {}'.format(response))

            if attempt == max_attempts:
                logging.error('Exhausted attempts!')
                return False

        self.max_position = response['min_position']

        if tweets:
            with self.db.atomic():
                Tweet.insert_many(tweets).on_conflict('IGNORE').execute()

        return True

    def _fetch(self):
        until = (self.until + timedelta(days=1)).replace(tzinfo=timezone.utc).astimezone(tz=None)

        params = {
            'f': 'realtime',
            'q': self.query + ' lang:en until:' + until.strftime('%Y-%m-%d'),
            'src': 'typd',
            'max_position': self.max_position
        }

        ua = self._USER_AGENTS[self.request_count % len(self._USER_AGENTS)]

        headers = {
            'User-Agent': ua,
            'X-Requested-With': "XMLHttpRequest"
        }

        r = self.session.get('http://twitter.com/i/search/timeline', params=params, headers=headers)

        return r.json(), ua

    def _process_tweets(self, tweet_it):
        tweets = []

        oldest = 0
        extracted = 0
        accepted = 0

        for tweet in tweet_it:
            oldest = min(oldest or tweet['date'], tweet['date'])
            extracted += 1

            if self._check_tweet(tweet):
                accepted += 1
                tweets.append(tweet)

        if extracted == 0:
            return None

        self.extracted_count += extracted
        self.accepted_count += accepted
        now = time.time()

        while now - self.time_mark >= 60:
            self.time_mark, _, _ = self.recent_stats.popleft()

        self.recent_stats.append((now, extracted, accepted))

        spent = (now - self.time_mark) / 3600
        extract_speed = sum(e for _, e, _ in self.recent_stats) / spent
        accept_speed = sum(a for _, _, a in self.recent_stats) / spent

        logging.info(
            'E: {:6} +{:2} {:5}/h   A: {:6} +{:2} {:5}/h   O: {}   R: {:4}   F: {}   J: {}'.format(
                self.extracted_count, extracted, int(extract_speed),
                self.accepted_count, accepted, int(accept_speed),
                datetime.fromtimestamp(oldest), self.request_count, self.fail_count, self.skip_count,
                self.extracted_count / spent
            )
        )

        self.until = datetime.utcfromtimestamp(oldest)

        return tweets

    def _extract_tweets(self, html):
        pq = PyQuery(html)

        for tweet_html in pq('div.js-stream-tweet'):
            yield self._extract_tweet(tweet_html)

    def _extract_tweet(self, html):
        pq = PyQuery(html)
        text = pq('p.js-tweet-text')

        for a in text('a'):
            a = PyQuery(a)

            if a.has_class('twitter-hashtag'):
                a.replace_with(' ' + a.text().replace('# ', '#') + ' ')
            elif a.has_class('twitter-atreply'):
                a.replace_with(' @' + a.attr('data-mentioned-user-id') + ' ')
            elif a.has_class('twitter-cashtag'):
                a.replace_with(' ' + a.text().replace('$ ', '$') + ' ')
            else:
                a.replace_with(' [link] ')

        return {
            'ticker': self.ticker,
            'id': pq.attr('data-tweet-id'),
            'date': int(pq('small.time span.js-short-timestamp').attr('data-time')),
            'user_id': int(pq('a.js-user-profile-link').attr('data-user-id')),
            'text': re.sub(r'\s+', ' ', text.text().strip()),
            'retweet_count': int(pq('span.ProfileTweet-action--retweet span.ProfileTweet-actionCount')
                                 .attr('data-tweet-stat-count').replace(',', '')),
            'favorite_count': int(pq('span.ProfileTweet-action--favorite span.ProfileTweet-actionCount')
                                  .attr('data-tweet-stat-count').replace(',', ''))
        }

    def _check_tweet(self, tweet):
        return tweet['retweet_count'] > 0 or tweet['favorite_count'] > 0

    def _get_oldest_date(self):
        try:
            oldest = (Tweet
                .select()
                .where(Tweet.ticker == self.ticker)
                .order_by(Tweet.date)
                .get())

            return oldest.date
        except DoesNotExist:
            return datetime.utcnow()
