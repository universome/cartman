import re
import logging
import time
import random
from datetime import datetime, timedelta, timezone
from collections import deque

from peewee import *
from requests import Session
from pyquery import PyQuery

class Tweet(Model):
    ticker = CharField(5)
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
    _USER_AGENTS = [
        '',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
    ]

    random.shuffle(_USER_AGENTS)

    def __init__(self, db, ticker, until=None):
        self.db = db
        self.session = Session()
        self.ticker = ticker
        self.max_position = ''

        # Statistics.
        self.time_mark = 0
        self.skip_count = 0
        self.request_count = 0
        self.fail_count = 0
        self.extracted_count = 0
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
        max_attempts = max(len(self._USER_AGENTS), 4)

        for attempt in range(max_attempts + 1):
            if attempt == max_attempts:
                self.until -= timedelta(days=1)
                logging.info('Skipping to {}...'.format(self.until))
                self.skip_count += 1
            elif attempt > 0:
                logging.info('Sleeping and retrying again...')
                time.sleep(2)

            response, user_agent = None, None

            try:
                response, user_agent = self._fetch()
            except Exception as ex:
                logging.exception('Error while fetching: {}'.format(ex))

            self.request_count += 1

            if response and 'items_html' in response and 'min_position' in response:
                html = response['items_html'].strip()

                if html and len(response['min_position']) > 16:
                    tweet_it = self._extract_tweets(html)
                    tweets = None

                    try:
                        tweets = self._process_tweets(tweet_it)
                    except Exception as ex:
                        logging.exception('Error while parsing: {}'.format(ex))

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
            'q': '${} lang:en until:{}'.format(self.ticker, until.strftime('%Y-%m-%d')),
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

        for tweet in tweet_it:
            oldest = min(oldest or tweet['date'], tweet['date'])
            extracted += 1
            tweets.append(tweet)

        if extracted == 0:
            return None

        self.extracted_count += extracted
        now = time.time()

        while self.recent_stats and now - self.time_mark >= 60:
            self.time_mark, _ = self.recent_stats.popleft()

        self.recent_stats.append((now, extracted))

        spent = (now - self.time_mark) / 3600
        extract_speed = sum(e for _, e in self.recent_stats) / spent

        logging.info(
            'E: {:6} +{:2} {:5}/h    O: {}    R: {:4}    F: {}    J: {}'.format(
                self.extracted_count, extracted, int(extract_speed),
                datetime.utcfromtimestamp(oldest),
                self.request_count, self.fail_count, self.skip_count,
                self.extracted_count / spent
            )
        )

        self.until = min(self.until, datetime.utcfromtimestamp(oldest))

        return tweets

    def _extract_tweets(self, html):
        pq = PyQuery(html)

        for tweet_html in pq('div.js-stream-tweet').not_('.withheld-tweet'):
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
