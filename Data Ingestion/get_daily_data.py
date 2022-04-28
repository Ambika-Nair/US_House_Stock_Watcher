import numpy as np

import pandas as pd
pd.set_option('display.max_columns', 0)
pd.set_option('display.max_colwidth', 0)

from house_stock_watcher import house_stock_url_generator, house_stock_data_loader, house_stock_data_transformer

import warnings
warnings.filterwarnings('ignore')

import json

import datetime as dt


#### STEP 1 - GET THE DAY / DATE ####

# api_link = 'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/'

# Even though the variable is named YESTERDAY, it gives us the data for 2 days ago i.e. DAY BEFORE YESTERDAY.
# Change the argument to "days = 2" below, if you want to check out the data for 25th April, 2022 which is LIVE.

yesterday = dt.date.today() - dt.timedelta(days=2)
print(yesterday)

#### STEP 2 - Creating the PROPER WORKING URL to be fed in ####

yesterday_link = house_stock_url_generator(yesterday.year, yesterday.month, yesterday.day)
print(yesterday_link)

#### STEP 3 - LOAD THE LATEST DATA ####

data_today = house_stock_data_loader(yesterday_link)

#print(data_today.info())


#### STEP 4 - TRANSFORM THE DATASET ###

data_today = house_stock_data_transformer(data_today)

print(data_today.info())


#### STEP 5 - THE TRANSFORMED DataFrame is ready to be pushed TO A CLOUD DATABASE ####
