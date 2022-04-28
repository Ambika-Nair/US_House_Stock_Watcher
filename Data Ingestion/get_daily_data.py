import numpy as np


import pandas as pd
pd.set_option('display.max_columns', 0)
pd.set_option('display.max_colwidth', 0)

from house_stock_watcher import house_stock_url_generator, house_stock_data_loader, house_stock_data_transformer
from digital_ocean_credentials import digital_ocean_host, digital_ocean_user, digital_ocean_password, digital_ocean_port
import mysql.connector

import warnings
warnings.filterwarnings('ignore')

import json
import re

import datetime as dt



#### STEP 1 - GET THE DAY / DATE ####

# api_link = 'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/'

# Even though the variable is named YESTERDAY, it gives us the data for 2 days ago i.e. DAY BEFORE YESTERDAY.
# Change the argument to "days = 2" below, if you want to check out the data for 25th April, 2022 which is LIVE.

yesterday = dt.date.today() - dt.timedelta(days=2)
#print(yesterday)

#### STEP 2 - Creating the PROPER WORKING URL to be fed in ####

yesterday_link = house_stock_url_generator(yesterday.year, yesterday.month, yesterday.day)
#print(yesterday_link)

#### STEP 3 - LOAD THE LATEST DATA ####

data_today = house_stock_data_loader(yesterday_link)

#print(data_today.info())


#### STEP 4 - TRANSFORM THE DATASET ###

data_today = house_stock_data_transformer(data_today)

#print(data_today.info())


#### STEP 5 - THE TRANSFORMED DataFrame is ready to be pushed TO A CLOUD DATABASE ####

### Connecting to the CLOUD SQL ###

connection = mysql.connector.connect(host= digital_ocean_host,
                                      user= digital_ocean_user,
                                      password= digital_ocean_password,
                                      port = digital_ocean_port,
                                      database='house_stock_watcher')
#print(connection)

cursor = connection.cursor()

#### STEP 6 - Creating the INSERT INTO Query for daily transactions

insert_query = "INSERT INTO transactions (disclosure_date, transaction_date, asset_description, ticker, type,amount, representative, district, cap_gains_over_200_usd) VALUES "

records = data_today.to_records(index=False)
result = list(records)

for i in range(len(result)):
    insert_query += (str(result[i]) + ",")

# Removing an EXTRA COMMA for the LAST ELEMENT in the string.
insert_query = insert_query[:-1]

# Using CONNECTION.COMMIT() actually reflects the changes in the Database TABLE.
# Without COMMIT command, data shall not be updated.
# (Currently, Commented to avoid a DOUBLE / DUPLICATE UPLOAD)

# Before Deploying, We must UNCOMMENT connection.commit() and just run it once per day.
# and make sure the argument DAYS = 2 above.

cursor.execute(insert_query)
#connection.commit()

#print(data_today.info())


