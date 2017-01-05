import requests
import json
import time

API_KEY = 'c575b69ae4e2408ab908c6f1711cf9a0'
URL = "https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key=" + API_KEY
START_YEAR = 2011
FINISH_YEAR = 2017

def load_articles():
    params = {'year': START_YEAR, 'month': 1}

    while params['year'] <= FINISH_YEAR and params['month'] <= 12:
        response = requests.get(URL.format(**params))

        try:
            data = response.json()
        except ValueError as e:
            print("Error occured when decoding response:", response.text)
            print("Url:", response.url)
            continue

        if not 'response' in data:
            print("Response is not valid:", response.text)
            print("Url:", response.url)
            continue

        save_articles(data['response'], params)

        if params['month'] != 12:
            params['month'] += 1
        else:
            params['month'] = 1
            params['year'] += 1

        time.sleep(1)

def save_articles(articles, params):
    name = "articles/articles_{month}-{year}.json".format(**params)

    with open(name, "w") as file:
        json.dump(articles, file)

    print("Created file — {}".format(name))

load_articles()
