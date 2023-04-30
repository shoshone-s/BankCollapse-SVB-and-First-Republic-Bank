import pandas as pd
import requests
from requests.adapters import HTTPAdapter
import time

s = requests.Session()
s.mount("https://", HTTPAdapter(max_retries=10))

locations = pd.DataFrame()
for state_abbr in ['AK','AL','AR','AS','AZ','CA','CO','CT','DC','DE','FL','FM','GA','GU','HI','IA','ID','IL','IN','KS','KY','LA','MA','MD','ME','MH','MI','MN','MO','MP','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA','PR','PW','RI','SC','SD','TN','TX','UT','VA','VI','VT','WA','WI','WV','WY']:
    print(state_abbr, end=' ')
    if state_abbr=='OR':
        r = s.get("https://banks.data.fdic.gov/api/locations?filters=STALP:\{}&limit=10000".format(state_abbr))
    else:
        r = s.get("https://banks.data.fdic.gov/api/locations?filters=STALP:{}&limit=10000".format(state_abbr))
    locations = pd.concat([locations, pd.json_normalize(r.json()['data'])])
    time.sleep(10)

if (locations.score==0).all():
    locations.drop(columns='score',inplace=True)
locations.columns = [x.replace('data.','') for x in locations.columns if x.startswith('data.')]

locations.to_csv('../../data/locations.csv', index=False)