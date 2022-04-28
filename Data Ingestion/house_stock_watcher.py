import numpy as np

import pandas as pd
pd.set_option('display.max_columns', 0)
pd.set_option('display.max_colwidth', 0)

import warnings
warnings.filterwarnings('ignore')

import json
import re

import datetime as dt




### FUNCTION 1 - URL GENERATOR FUNCTION

def house_stock_url_generator(year, month, day):
    if (month < 10 and day < 10):
        link = f'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/transaction_report_for_0{month}_0{day}_{year}.json'

    elif (month < 10 and day >= 10):
        link = f'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/transaction_report_for_0{month}_{day}_{year}.json'

    elif (month >= 10 and day < 10):
        link = f'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/transaction_report_for_{month}_0{day}_{year}.json'

    # elif month >= 10 and day >= 10:
    else:
        link = f'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/transaction_report_for_{month}_{day}_{year}.json'

    return link


### FUNCTION 2 - RAW DATA LOADER

def house_stock_data_loader(link):
    try:
        df_today = pd.read_json(link)
        return df_today
    except:
        pass
    # HERE WE MAY WANNA SET AN ALERT OR SOMETHING such as NO NEW DATA UPLOADED today via EMAIL.
    # OR
    # In case of an ERROR in schema, data type, data quality etc, an e-mail can be sent too. (SMTP library for emails)


### FUNCTION 3 - RAW DATA TRANSFORMER

def house_stock_data_transformer(dataframe):

    dataframe['transactions'] = dataframe['transactions'].apply(lambda x: list(x))
    dataframe = dataframe.explode('transactions')
    dataframe.reset_index(inplace=True, drop=True)

    dataframe_2 = pd.json_normalize(dataframe['transactions'])

    dataframe_new = pd.concat([dataframe, dataframe_2], axis=1)
    dataframe_new = dataframe_new[['filing_date', 'transaction_date', 'description', 'ticker', 'transaction_type',
                                   'amount', 'name', 'district', 'cap_gains_over_200']]

    dataframe_new.columns = ['disclosure_date', 'transaction_date', 'asset_description', 'ticker', 'type',
                             'amount', 'representative', 'district', 'cap_gains_over_200_usd']


    dataframe_new.asset_description = dataframe_new.asset_description.apply(lambda x: str(x))
    dataframe_new.asset_description = dataframe_new.asset_description.apply(lambda x: re.sub("'","",x))

    dataframe_new.representative = dataframe_new.representative.apply(lambda x: str(x))
    dataframe_new.representative = dataframe_new.representative.apply(lambda x: re.sub("'","",x))

    dataframe_new['transaction_date'] = pd.to_datetime(dataframe_new['transaction_date'], yearfirst=True, errors='ignore')

    dataframe_new['disclosure_date'] = pd.to_datetime(dataframe_new['disclosure_date'],yearfirst = True, errors = 'ignore')
    dataframe_new = dataframe_new.sort_values(by='transaction_date', ascending=False)
    dataframe_new.reset_index(inplace=True, drop=True)

    return dataframe_new
