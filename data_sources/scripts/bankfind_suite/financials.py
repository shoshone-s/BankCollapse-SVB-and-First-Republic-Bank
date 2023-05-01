import pandas as pd
import requests
from requests.adapters import HTTPAdapter
import time

s = requests.Session()
s.mount("https://", HTTPAdapter(max_retries=10))

assets = pd.DataFrame()