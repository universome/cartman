import re
import logging
from datetime import datetime
import urllib

import requests

from base.database import db

class Scraper:
    def __init__(self, ticker, continuation=''):
        self.ticker = ticker
        self.continuation = continuation
        self.extracted = 0

        db.create_tables([News], safe=True)

    def scrape(self):
        while self.continuation is not None:
            self._step()

    def _step(self):
        response = self._fetch()

        news = (self._extract_news(item) for item in response['items'])
        news = list(filter(None, news))

        self.continuation = response.get('continuation')

        self.extracted += len(news)
        oldest = datetime.utcfromtimestamp(min(n['date'] for n in news))

        logging.info('Extracted {} (+{}) news, oldest: {}, continuation: {}'.format(
            self.extracted, len(news), oldest, self.continuation
        ))

        with db.atomic():
            for i in range(0, len(news), 100):
                News.insert_many(news[i:i+100]).on_conflict('IGNORE').execute()

    def _fetch(self):
        r = requests.get('http://cloud.feedly.com/v3/streams/contents', params={
            'streamId': 'feed/http://finance.yahoo.com/rss/headline?s=' + self.ticker,
            'count': 1000,
            'continuation': self.continuation
        })

        return r.json()

    _INVALID_URL_SUFFIXES = [
        'finance/news/rss/story/*&',
        'rss/SIG=102irdsec/*?',
        'rss/SIG=10qegkfse/*?'
    ]

    _MEDIA_TITLE_PREFIXES = [
        '[video]', '[audio]', '[podcast]', '[watch]'
    ]

    def _extract_news(self, item):
        # Broken news.
        if 'alternate' not in item and 'RSS feed not found' in item['title']:
            return None

        if item['title'] == '*** DATA NOT AVAILABLE ***':
            return None

        title_lc = item['title'].lower()

        if any(title_lc.startswith(prefix) for prefix in self._MEDIA_TITLE_PREFIXES):
            return None

        alt_href = item['alternate'][0]['href']
        summary = item['summary']['content'].strip() if 'summary' in item else None

        marked = item['title'].startswith('[$$]')
        title = (item['title'][len('[$$]'):] if marked else item['title']).strip()

        description = summary and re.sub(r'^\[.+?\]\s*-\s*', '', summary)

        match = re.search(r'finance/(news|external/(.+?))/', alt_href)
        if not match:
            return None

        source = match.group(2) or summary and re.search(r'^(\[.+?\])\s*-', summary).group(1)

        url = item.get('canonicalUrl')
        if not url:
            match = re.search(r'(https?(:|%3A)//.+?)(\?|#|$)', alt_href[7:])

            # Broken url.
            if not match and any(suffix in alt_href for suffix in self._INVALID_URL_SUFFIXES):
                url = None
            else:
                url = match.group(1) if match.group(2) == ':' else urllib.parse.unquote(match.group(1))

        return {
            'ticker': self.ticker,
            'id': int(item['originId'][len('yahoo_finance/'):]),
            'date': item['published'] // 1000,
            'source': source,
            'title': title,
            'description': description,
            'url': url,
            'engagement': item.get('engagement'),
            'marked': marked
        }
