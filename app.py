from flask import Flask, jsonify
from data_sources.scripts.api_scripts import secEDGAR_api

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