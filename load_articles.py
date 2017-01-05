import requests
import json
import time

API_KEY = 'c575b69ae4e2408ab908c6f1711cf9a0'
COMPANIES = ['google', 'walmart', 'microsoft', 'international business machines', 'general electric']
BASE_URL = "https://api.nytimes.com/svc/search/v2/articlesearch.json"
START_YEAR = 2015
FINISH_YEAR = 2017

def load_articles(company):
	articles = []
	year = START_YEAR

	while year <= FINISH_YEAR:
		articles += load_articles_by_year(company, year)
		year += 1

	return articles

def load_articles_by_year(company, year):
	articles = []
	params = {
		'page': 0,
		'begin_date': str(year) + '0101',
		'end_date': str(year + 1) + '0101',
		'api-key': API_KEY,
		'fq': 'organizations:\"{}\"'.format(company),
		'sort': 'oldest'
	}

	while True:
		response = requests.get(BASE_URL, params=params)

		try:
			data = response.json()
		except ValueError as e:
			print("Error occured when decoding response:", response.text)
			print("Url:", response.url)
			continue

		if 'message' in data and data['message'] == 'API rate limit exceeded':
			print("API rate limit exceeded for url:", response.url)
			time.sleep(5)
			continue

		if not 'status' in data or data['status'] != "OK":
			print("Loading articles for {} ({}) and page {} failed: {}".format(
				company,
				year,
				params['page'],
				data
			))
			print("Url:", response.url)
			continue

		print("Loaded {}/{} articles for {} ({})".format(
			data['response']['meta']['offset'] + len(data['response']['docs']),
			data['response']['meta']['hits'],
			company,
			year
		))

		articles = articles + data['response']['docs']

		if params['page'] * 10 >= data['response']['meta']['hits']:
			break

		params['page'] += 1
		time.sleep(1)

	return articles


def save_articles(articles, company):
	name = "articles/{}_articles_{}-{}.json".format(company, START_YEAR, FINISH_YEAR)

	with open(name, "w") as file:
		json.dump(articles, file)

	print("Created file — {}".format(name))

save_articles(load_articles(COMPANIES[0]), COMPANIES[0])
