import requests
import json

# Replace 'YOUR_API_TOKEN' with your actual IEX Cloud API token
API_TOKEN = 'sk_5f651f2811df49288f11de6379f58cc0'

def extract_iex():
    symbol = 'SIVB'  # Silicon Valley Bank stock symbol

    # API endpoint URL
    url = f'https://cloud.iexapis.com/stable/stock/{symbol}/chart/max'

    # Request headers
    headers = {
        'Content-Type': 'application/json'
    }

    # Request parameters
    params = {
        'token': API_TOKEN
    }

    # Make the API request
    response = requests.get(url, headers=headers, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Retrieve the data from the response
        data = response.json()

        filename = 'iex_data.json'
        with open(filename, 'w') as file:
            json.dump(data, file)

    else:
        print('Error occurred during API request:', response.status_code)

def load(): 
    print()