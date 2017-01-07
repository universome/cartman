import os
from itertools import islice

from requests import Session

# TODO: send appid.
def sentiment140(it, subject='', chunk_size=10000):
    it = iter(it)
    session = Session()

    while True:
        chunk = list(islice(it, chunk_size))

        if not chunk:
            break

        r = session.post('http://www.sentiment140.com/api/bulkClassify',
                         params={
                             'query': subject,
                             'appid': os.environ['S140_APP_ID']
                         },
                         data='\n'.join(chunk))

        for line, actual in zip(r.text.splitlines(), chunk):
            polarity, text = line.split(',')

            assert text[1:-1] == actual

            yield int(polarity[1]) // 2 - 1
