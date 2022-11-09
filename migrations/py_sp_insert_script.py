#Importing required libraries

from snowflake.snowpark import Session
from snowflake.snowpark.types import StructField()

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

## Define the function for the Stored Procedure

def insert_into_table(snowpark_session: session):
    snowpark_session.sql('''
                        create or replace TABLE OIL_AND_GAS.CONFORMED.CUSTOMER_TABLE 
                        (
                            CUSTOMER_ID     NUMBER(38,0),
                            DATE            VARCHAR(16777216),
                            SALUTATION      VARCHAR(5),
                            FIRST_NAME      VARCHAR(200),
                            LAST_NAME       VARCHAR(200),
                            DATE_INSERT     TIMESTAMP_NTZ(9)
                        ); 
                        ''').collect()
  

    snowpark_session.sql('''
                        INSERT INTO OIL_AND_GAS.CONFORMED.CUSTOMER_TABLE
                                (
                                    CUSTOMER_ID,         
                                    DATE,                
                                    SALUTATION,          
                                    FIRST_NAME,          
                                    LAST_NAME,
                                    DATE_INSERT
                                 )
                        SELECT C_CUSTOMER_SK
                                    , TO_CHAR(D_DATE, 'YYYYMMDD')
                                    , C_SALUTATION
                                    , C_FIRST_NAME
                                    , C_LAST_NAME
                                    , CURRENT_TIMESTAMP()
                        FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER C
                        JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM D 
                        ON D_DATE_SK = C_FIRST_SHIPTO_DATE_SK
                        LIMIT 100
                        ''').collect()
  
    ## Execute a star select query into a Snowflake dataframe
    results = snowpark_session.sql('SELECT * FROM OIL_AND_GAS.CONFORMED.CUSTOMER_TABLE').collect()

    return results

## Register Stored Produre in Snowflake
## Add packages and data types

session.add_packages('snowflake-snowpark-python')

### Upload Stored Produre to Snowflake
session.sproc.register(
    func = insert_into_table
  , return_type = StructField()
  , input_types = []
  , is_permanent = True
  , name = 'SNOWPARK_insert_into_table'
  , replace = True
  , stage_location = '@MY_STAGE'
)