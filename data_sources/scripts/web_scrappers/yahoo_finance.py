import requests
from bs4 import BeautifulSoup

URL = "https://finance.yahoo.com"
page = requests.get(URL)

soup = BeautifulSoup(page.content, "html.parser")

print(soup.find(id="mrt-node-Lead-3-FinanceHeader").text) 
