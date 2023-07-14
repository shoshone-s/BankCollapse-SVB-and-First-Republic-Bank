#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd

#reading of csv files
df_balsheet = pd.read_csv("raw_data/balance_sheet.csv")
df_cashflow = pd.read_csv("raw_data/cash_flow.csv")
df_incomest = pd.read_csv("raw_data/income_statement.csv")
df_stockprice = pd.read_csv("raw_data/stock_price_daily.csv")
df_svbdebt = pd.read_csv("raw_data/svb_debt.csv")
df_company = pd.read_csv('raw_data/company_overview.csv')


# Balance Sheet Data

# In[2]:


#separate categorical and numerical columns
df_balsheet.dtypes == 'object'
#by using the dtypes function and equality operator we can get which columns are objects(categorical data) and which are not


# In[3]:


#list out the categorical columns and the numerical columns separately

balsheet_num_vars = df_balsheet.columns[df_balsheet.dtypes != 'object']
balsheet_cat_vars = df_balsheet.columns[df_balsheet.dtypes == 'object']
print(balsheet_num_vars)
print(balsheet_cat_vars)


# In[4]:


#listing of all numerical columns 
df_balsheet[balsheet_num_vars]


# In[5]:


# A count of all the missing values, summed up and sorted by column with the highest missing values
df_balsheet[balsheet_num_vars].isnull().sum().sort_values(ascending=False)


# Cash Flow Data

# In[6]:


df_cashflow.dtypes == 'object'


# In[7]:


#list out the categorical columns and the numerical columns separately

cashflow_num_vars = df_cashflow.columns[df_cashflow.dtypes != 'object']
cashflow_cat_vars = df_cashflow.columns[df_cashflow.dtypes == 'object']
print(cashflow_num_vars)
print(cashflow_cat_vars)


# In[8]:


#listing of all numerical columns 
df_cashflow[cashflow_num_vars]


# In[9]:


# A count of all the missing values, summed up and sorted by column with the highest missing values
df_cashflow[cashflow_num_vars].isnull().sum().sort_values(ascending=False)


# Income Statement Data

# In[10]:


df_incomest.dtypes == 'object'


# In[11]:


#list out the categorical columns and the numerical columns separately

incomest_num_vars = df_incomest.columns[df_incomest.dtypes != 'object']
incomest_cat_vars = df_incomest.columns[df_incomest.dtypes == 'object']
print(incomest_num_vars)
print(incomest_cat_vars)


# In[12]:


#listing of all numerical columns 
df_incomest[incomest_num_vars]


# In[13]:


# A count of all the missing values, summed up and sorted by column with the highest missing values
df_incomest[incomest_num_vars].isnull().sum().sort_values(ascending=False)


# Stock Price Data

# In[14]:


df_stockprice.dtypes == 'object'


# In[15]:


#list out the categorical columns and the numerical columns separately

stockprice_num_vars = df_stockprice.columns[df_stockprice.dtypes != 'object']
stockprice_cat_vars = df_stockprice.columns[df_stockprice.dtypes == 'object']
print(stockprice_num_vars)
print(stockprice_cat_vars)


# In[16]:


#listing of all numerical columns 
df_stockprice[stockprice_num_vars]


# In[17]:


# A count of all the missing values, summed up and sorted by column with the highest missing values
df_stockprice[stockprice_num_vars].isnull().sum().sort_values(ascending=False)


# SVB Debt To Equity Ratio

# In[18]:


df_svbdebt.dtypes == 'object'


# In[19]:


#list out the categorical columns and the numerical columns separately

svbdebt_num_vars = df_svbdebt.columns[df_svbdebt.dtypes != 'object']
svbdebt_cat_vars = df_svbdebt.columns[df_svbdebt.dtypes == 'object']
print(svbdebt_num_vars)
print(svbdebt_cat_vars)


# In[20]:


#listing of all numerical columns 
df_svbdebt[svbdebt_num_vars]


# In[21]:


# A count of all the missing values, summed up and sorted by column with the highest missing values
df_svbdebt[svbdebt_num_vars].isnull().sum().sort_values(ascending=False)


# In[22]:


#income statement
incomest_date = df_incomest.fiscalDateEnding


# In[23]:


#stock data
stock_date = df_stockprice.date


# In[24]:


#debt to equity ratio
debt_date = df_svbdebt.Date


# In[25]:


def date_check(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame):    
    begin_date = '2017-01-01'
    end_date = '2022-01-01'
    
    debt_after_start_date = debt_date >= begin_date
    debt_before_end_date = debt_date <= end_date
    debt_between_dates = debt_after_start_date & debt_before_end_date
    
    income_after_start_date = incomest_date >= begin_date
    income_before_end_date = incomest_date <= end_date
    income_between_dates = income_after_start_date & income_before_end_date
    
    stock_after_start_date = stock_date >= begin_date
    stock_before_end_date = stock_date <= end_date
    stock_between_dates = stock_after_start_date & stock_before_end_date
    
    df2_debt = debt_date.loc[debt_between_dates]
    count_dates_svbdebt = df2_debt.value_counts().sum()
    
    df2_income = incomest_date.loc[income_between_dates]
    count_dates_income = df2_income.value_counts().sum()
        
    df2_stock = stock_date.loc[stock_between_dates]
    count_dates_stock = df2_stock.value_counts().sum()

    return count_dates_svbdebt, count_dates_income, count_dates_stock


# In[26]:


#date check count
total_svbdebt_date_count, total_income_date_count, total_stock_date_count = date_check(debt_date, incomest_date, stock_date)

print(f"Total dates in Debt To Equity Ratio that are outside of 2017-01-01 to 2022-01-01: {total_svbdebt_date_count}")
print(f"Total dates in Income Statement that are outside of 2017-01-01 to 2022-01-01: {total_income_date_count}")
print(f"Total dates in Stock Price History that are outside of 2017-01-01 to 2022-01-01: {total_stock_date_count}")


# In[27]:


company_num_vars = df_company.columns[df_company.dtypes != 'object']
company_cat_vars = df_company.columns[df_company.dtypes == 'object']
print(company_num_vars)
print(company_cat_vars)


# In[28]:


#checking for missing values not NaN in the symbol column
def missing_symbols():
    missing_symbols = df_company['Symbol'].isnull().values.any()
    if missing_symbols == True:
        raise RuntimeError('Symbol Not Found')
    


# In[ ]:




