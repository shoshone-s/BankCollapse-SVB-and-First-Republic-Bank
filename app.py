from flask import Flask, jsonify
from data_sources.scripts.api_scripts import secEDGAR_api
<<<<<<< HEAD
import os
import configparser
from dotenv import dotenv_values
import redshift_connector

# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read("keys_config.cfg")

AWS_ACCESS_KEY_ID = cfg_data["AWS"]["access_key_id"]
AWS_SECRET_ACCESS_KEY = cfg_data["AWS"]["secret_access_key"]

S3_BUCKET_NAME = cfg_data["S3"]["bucket_name"]

REDSHIFT_DB_NAME = cfg_data["Redshift"]["database_name"]
REDSHIFT_WORKGROUP_NAME = cfg_data["Redshift"]["workgroup_name"]
IAM_REDSHIFT = cfg_data["Redshift"]["iam_role"]
REDSHIFT_REGION_NAME = cfg_data["Redshift"]["region_name"]
=======
>>>>>>> origin/main

app = Flask(__name__)

@app.route('/debtToEquity')
def get_debt_to_equity():
    # TODO: Add connection to database 

    return 'this will return a json object of the debtToEquity table'

@app.route('/pricehistory')
def get_price_history():
    return 'this will return a json object of pricehistory table'

@app.route('/symbol')
def get_symbol():
    return 'this will return a json object of the symbol table'

@app.route('/location')
def get_location():
    return 'this will return a json object of the location table'

@app.route('/financials')
def get_financials():
    return 'this will return a json object of the financials table'

@app.route('/company')
def get_company():
    return 'this will return a json object of the company table'

@app.route('/secdata/ticker=<ticker_symbol>')
def get_secData(ticker_symbol):
<<<<<<< HEAD
    # conn = redshift_connector.connect(
    #     host=
    # )
=======
>>>>>>> origin/main
    cfg_head = secEDGAR_api.headers
    tickerInfo = secEDGAR_api.companyTickerData(cfg_head, ticker_symbol)
    data = secEDGAR_api.secData(tickerInfo['cik_str'])
    return jsonify(data)

@app.route('/')
def home():
    return 'Team 20 API'

# uncomment when deploying to prod
# app.run(host='0.0.0.0', port=81)

if __name__=='__main__':
    app.run(debug=True)