from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Set up the Selenium web driver
# service = Service('path/to/chromedriver')
# options = Options()
# options.headless = True  # Run Chrome in headless mode (no GUI)
# driver = webdriver.Chrome(service=service, options=options)

options = webdriver.ChromeOptions()
options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
chrome_driver_binary = "/usr/local/bin/chromedriver"
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

# Print the link
print('Export Link:', link)

# Quit the web driver

