import requests
from pyquery import PyQuery as pq

article_url = "http://www.nytimes.com/2017/01/04/science/hurricanes-us.html"
headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'}

article = pq(url=article_url, opener=lambda url: requests.get(url, headers=headers).text)

with open('kek.txt', 'w') as file:
	file.write(article("#story .story-body").text())

print("Done!")
