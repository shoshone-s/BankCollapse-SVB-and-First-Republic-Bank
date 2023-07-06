import pandas as pd
import requests
from requests.adapters import HTTPAdapter
import time

s = requests.Session()
s.mount("https://", HTTPAdapter(max_retries=10))

institutions = pd.DataFrame()
total = s.get("https://banks.data.fdic.gov/api/institutions").json()['totals']['count']
limit = 10000
offset = 0
while len(institutions) < total:
    r = s.get("https://banks.data.fdic.gov/api/institutions?sort_by=ID&offset={}&limit={}".format(offset, limit))
    institutions = pd.concat([institutions, pd.json_normalize(r.json()['data'])])
    offset += limit
    time.sleep(5)

institutions.columns = [x.replace('data.', '') for x in institutions.columns]
institutions.to_csv('../../data/institutions.csv', index=False)