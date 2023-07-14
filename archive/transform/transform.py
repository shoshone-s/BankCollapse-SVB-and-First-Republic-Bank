"""
    Transform raw data and load clean csvs to s3 bucket
"""

import pandas as pd
import os
import configparser
import aws_read_write



TABLE_COLUMN_NAMES = {
    'company': ['id', 'name', 'symbolid', 'FDICCertID', 'Class', 'Status'],
    'debt_to_equity': ['ID', 'Date', 'LongTermDebt', 'DebtEquityRatio'],
    'financials': ['ID', 'report_date', 'total_assets', 'total_liabilities', 'total_debt', 'assets_return', 'equity_return', 'efficiency', 'risk_base_capital_ratio'],
    'location': ['ID', 'cert', 'company_name', 'main_office', 'branch_name', 'established_date', 'service_type', 'address', 'county', 'city', 'state', 'zip', 'latitude', 'longitude'],
    'price_history': ['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume'],
    'sec_data': ['ID', 'asset_num', 'company_id', 'report_type', 'start_date', 'end_date', 'date_filed', 'fiscal_year', 'fiscal_period', 'form', 'frame', 'value'],
    'symbol': ['ID', 'TickerSymbol', 'Name'],
}

# location of data files

