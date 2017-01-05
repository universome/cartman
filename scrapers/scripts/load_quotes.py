import requests

TICKERS = ['MMM', 'T', 'ADBE', 'AA', 'GOOG', 'AXP', 'AIG', 'AMT', 'AAPL', 'AMAT', 'BAC', 'BA', 'CA', 'CAT', 'CVX', 'CSCO', 'C', 'KO', 'GLW', 'DD', 'EMC', 'XOM', 'FSLR', 'GE', 'HPQ', 'HD', 'IBM', 'IP', 'INTC', 'JPM', 'JNJ', 'MCD', 'MRK', 'MSFT', 'PFE', 'PG', 'TRV', 'UTX', 'VZ', 'WMT', 'DIS', 'WFC', 'YHOO', 'YNDX']
INTERVAL_IDS = {60: 2, 300: 3, 600: 4, 900: 5, 1800: 6, 3600: 7}
INTERVAL_TO_YEAR = {2: 2016, 3: 2015, 4: 2014, 5: 2013, 6: 2012, 7: 2011}
BASE_URL = 'http://export.finam.ru/{ticker}.csv?market=25&em=20590&code={ticker}&apply=0&df={day}&mf={month}&yf={year}&p={interval_id}&f=lol&e=.csv&cn={ticker}&dtf=1&tmf=1&MSOR=1&mstime=on&mstimever=1&sep=1&sep2=1&datf=1&at=1'

def load_quotes(ticker, interval):
    params = {
        'ticker': ticker,
        'interval_id': INTERVAL_IDS[interval],
        'year': INTERVAL_TO_YEAR[INTERVAL_IDS[interval]],
        'month': 0,
        'day': 1
    }

    print("Getting {ticker} ({interval_id}) since {day}.{month}.{year}".format(**params))

    response = requests.get(BASE_URL.format(**params)).text

    save_quotes(response, {
        'ticker': ticker,
        'interval': interval,
        'day': params['day'],
        'month': params['month'],
        'year': params['year']
    })

def save_quotes(quotes, params):
    name = "quotes/{ticker}_{day}-{month}-{year}_({interval}).csv".format(**params)

    with open(name, "w") as file:
        file.write(quotes)

    print("Created file — {}".format(name))

for ticker in TICKERS:
    for interval in INTERVAL_IDS:
        load_quotes(ticker, interval)

