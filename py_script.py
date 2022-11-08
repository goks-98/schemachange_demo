#Import required libraries
import os
from snowflake.snowpark import Session
import requests
import pandas as pd
import json

#Connecting to snowflake
connection_parameters = {
"account": "ld77469.uae-north.azure",
"user": "GOKS98",
"password": "March20311",
"role": "ACCOUNTADMIN",
"warehouse": "SNOWFLAKE_WAREHOUSE",
"database": "OIL_AND_GAS",
"schema": "CONFORMED"
 }

session = Session.builder.configs(connection_parameters).create()

#Creating a Dataframe
df_table = session.table("SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WAREHOUSE")

#Converting to pandas df to make manipulation easier
df_pd = df_table.to_pandas()

#Getting weather data from API for every zipcode
json_list = []
for i in df_pd['W_ZIP']:
    response = requests.get(f"http://api.openweathermap.org/geo/1.0/zip?zip={i},US&appid=b3effbd4d36024a4b9735d33830e7ca3")
    data = response.json()
    json_list.append(data)
json_list

#Creating a new df with weather info and zip code
zip_code = df_pd['W_ZIP']
data = list(zip(zip_code, json_list))
weather_df = pd.DataFrame(data=data, columns=['W_ZIP', 'WEATHER_JSON'])

#Joining weather nad warehouse tables
snowflake_df = df_pd.merge(weather_df, how='inner', on='W_ZIP')

snowflake_df_1 = session.create_dataframe(snowflake_df)
#Writing table to snowflake
snowflake_df_1.write.mode("overwrite").save_as_table("SCM_PROD.CONFORMED.w_warehouse_weather")
