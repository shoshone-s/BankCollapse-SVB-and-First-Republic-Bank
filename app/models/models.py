from app import db_engine, Base
from sqlalchemy import ForeignKey, Column, String, Integer, CHAR, VARCHAR, UUID, DATETIME, DATE, BOOLEAN



class Company(Base):
    __tablename__ = 'companies'

    id = Column('id', Integer, primary_key=True)
    name = Column('name', VARCHAR)
    symbol_id = Column('symbol_id', UUID, ForeignKey('symbols.id'))
    fdiccert_id = Column('fdiccert_id', Integer)
    company_class = Column('class', VARCHAR)
    status = Column('status', VARCHAR)

    def __init__(self, id, name, symbol_id, fdiccert_id, company_class, status):
        self.id = id
        self.name = name
        self.symbol_id = symbol_id
        self.fdiccert_id = fdiccert_id
        self.compnay_class = company_class
        self.status = status

    def __repr__(self):
        return f"('Id:' {self.id}, 'Name:' {self.name}, 'Symbol:' {self.symbol_id})"

class Symbol(Base):
    __tablename__ = 'symbols'

    id = Column('id', UUID, primary_key=True)
    ticker_symbol = Column('tickerr_symbol', VARCHAR)
    name = Column('name', VARCHAR)

    def __init__(self, id, ticker_symbol, name):
        self.id = id
        self.ticker_symbol = ticker_symbol
        self.name = name

    def __repr__(self):
        return f"('Id:' {self.id}, 'Ticker:' {self.ticker_symbol}, 'Name:' {self.name})"

class DebtToEquity(Base):
    __tablename__ = 'debt_to_equity'

    id = Column('id', UUID, primary_key=True)
    date = Column('date', DATE)
    debtToEquityRatio = Column('ratio', Integer)

    def __init__(self, id, date, debtToEquityRatio):
        self.id = id
        self.date = date
        self.debtToEquityRatio = debtToEquityRatio

    def __repr__(self):
        return f"('Id:' {self.id}, 'Date:' {self.date}, 'Ratio:' {self.debtToEquityRatio})"

class PriceHistory(Base):
    __tablename__ = 'price_history'

    id = Column('id', UUID, primary_key=True)
    symbol_id = Column('symbol_id', UUID, ForeignKey('symbols.id'))
    datetime = Column('datetime', DATETIME)
    open = Column('open', Integer)
    high = Column('high', Integer)
    low = Column('low', Integer)
    close = Column('close', Integer)
    adj_close = Column('adj_close', Integer)
    volume = Column('volume', Integer)
    debtToEquityID = Column('debtToEquityID', UUID, ForeignKey('debt_to_equity.id'))

    def __init__(self, id, symbol_id, datetime, open, high, low, close, adj_close, volume, debtToEquityID):
        self.id = id
        self.symbol_id = symbol_id
        self.datetime = datetime
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.adj_close = adj_close
        self.volume = volume
        self.debtToEquityID = debtToEquityID

    def __repr__(self):
        return f"('Id:' {self.id}, 'Symbol_Id:' {self.symbol_id}, 'DebtToEquityID:' {self.debtToEquityID})"

class secData(Base):
    __tablename__ = 'secData'

    id = Column('id', UUID, primary_key=True)
    company_id = Column('company_id', UUID, ForeignKey('companies.id'))
    asset_num = Column('asset_num', String)
    report_type = Column('report_type', String)
    start_date = Column('start_date', DATE)
    end_date = Column('end_date', DATE)
    date_filed = Column('date_filed', DATE)
    fiscal_year = Column('fiscal_year', Integer)
    fiscal_period = Column('fiscal_period', String)
    form = Column('form', String)
    frame = Column('frame', String)
    value = Column('value', String)

    def __init__(self, id, company_id, asset_num, report_type, start_date, end_date, date_filed, fiscal_year, fiscal_period, form, frame, value):
        self.id = id
        self.company_id = company_id
        self.asset_num = asset_num
        self.report_type = report_type
        self.start_date = start_date
        self.end_date = end_date
        self.date_filed = date_filed
        self.fiscal_year = fiscal_year
        self.fiscal_period = fiscal_period
        self.form = form
        self.frame = frame
        self.value = value

    def __repr__(self):
        return f"('Id:' {self.id}, 'CompanyID:' {self.company_id}, 'Form:' {self.form}, 'Value:' {self.value})"


class Location(Base):
    __tablename__ = 'loctions'

    id = Column('id', UUID, primary_key=True)
    company_id = Column('company_id', UUID, ForeignKey('companies.id'))
    main_office = Column('main_office', BOOLEAN)
    office_name = Column('office_name', String)
    address = Column('address', String)
    country = Column('country', String)
    city = Column('city', String)
    state = Column('state', String)
    zip_code = Column('zip_code', String)
    latitude = Column('latitude', String)
    longitude = Column('longitude', String)
    estabished_date = Column('estabisehd_date', String)

    def __init__(self, id, company_id, main_office, office_name, address, country, city, state, zip_code, latitude, longitude, established_date):
        self.id = id
        self.company_id = company_id
        self.main_office = main_office
        self.office_name = office_name
        self.address = address
        self.country = country
        self.city = city
        self.state = state
        self.zip_code = zip_code
        self.latitude = latitude
        self.longitude = longitude
        self.estabished_date = established_date

    def __repr__(self):
        return f"('Id:' {self.id}, 'CompanyID:' {self.company_id}, 'OfficeName:' {self.office_name}, 'DateEstablished:' {self.estabished_date})"


class Financials(Base):
    __tablename__ = 'financials'

    id = Column('id', UUID, primary_key=True)
    company_id = Column('company_id', UUID, ForeignKey('companies.id'))
    report_date = Column('report_date', DATE)
    total_assets = Column('total_assets', Integer)
    total_liabilities = Column('total_liabilities', Integer)
    total_debt = Column('total_debt', Integer)
    assets_return = Column('assets_return', Integer)
    equity_return = Column('equity_return', Integer)
    efficiency = Column('efficiency', Integer)
    risk_base_capital_ratio = Column('risk_base_capital_ratio', Integer)

    def __init__(self, id, company_id, report_date, total_assets, total_liabilities, total_debt, assets_return, equity_return, efficiency, risk_base_capital_ration):
        self.id = id
        self.company_id = company_id
        self.report_date = report_date
        self.total_assets = total_assets
        self.total_liabilities = total_liabilities
        self.total_debt = total_debt
        self.assets_return = assets_return
        self.equity_return = equity_return
        self.efficiency = efficiency
        self.risk_base_capital_ratio = risk_base_capital_ration

    def __repr__ (self):
        return f"('Id:' {self.id}, 'CompanyID:' {self.company_id}, 'ReportDate:' {self.report_date}, 'RiskBaseCapitalRatio:' {self.risk_base_capital_ratio})"

