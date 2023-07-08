from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import requests


options = webdriver.ChromeOptions()
options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" # TODO: replace with function that finds each person's respective Chrome browser 
chrome_driver_binary = "/usr/local/bin/chromedriver" # TODO: replace with function that finds each person's respective Chrome browser 
driver = webdriver.Chrome(chrome_driver_binary, chrome_options=options)

# Navigate to the webpage
url = 'https://www.spglobal.com/spdji/en/indices/equity/dow-jones-us-financial-services-index/#overview'
driver.get(url)

# Wait for the link to be available
link_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a.export')))

# Dismiss the cookie consent banner if present
cookie_consent_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'onetrust-accept-btn-handler')))
cookie_consent_button.click()

# Wait for the export button to be clickable
export_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.export')))

# Click the export button
export_button.click()

# Wait for the link to be available
link_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a.export')))

# Retrieve the link
link = link_element.get_attribute('href')

# Quit the web driver

# Download file onto my local machine 

# # URL of the Excel file
# url = 'https://www.spglobal.com/spdji/en/idsexport/file.xls?hostIdentifier=48190c8c-42c4-46af-8d1a-0cd5db894797&redesignExport=true&languageId=1&selectedModule=PerformanceGraphView&selectedSubModule=Graph&yearFlag=oneYearFlag&indexId=101113936'

url = str(link)

# Send a GET request to download the file
response = requests.get(link)

# Check if the request was successful
if response.status_code == 200:
    # Retrieve the filename from the response headers
    content_disposition = response.headers.get('content-disposition')
    filename = content_disposition.split('filename=')[1].strip('"')

    # Save the file locally
    with open(filename, 'wb') as file:
        file.write(response.content)

    print(f'Successfully downloaded the file as: {filename}')
else:
    print('Failed to download the file.')
 

