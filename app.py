from flask import Flask

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

@app.route('/locations')
def get_locations():
    return 'this will return a json object of the locations table'

@app.route('/financials')
def get_financials():
    return 'this will return a json object of the financials table'

@app.route('/company')
def get_company():
    return 'this will return a json object of the company table'

@app.route('/secdata')
def index():
    return 'this will return a json object of the secdata table'

@app.route('/')
def home():
    return 'Team 20 API'

app.run(host='0.0.0.0', port=81)