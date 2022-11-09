## Define the function for the Stored Procedure
import snowflake.snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.types import StringType

#Connecting to snowflake
connection_parameters = {
"account": "ld77469.uae-north.azure",
"user": "GOKS98",
"password": "March20311",
"role": "ACCOUNTADMIN",
"warehouse": "SNOWFLAKE_WAREHOUSE",
"database": "SCM_PROD",
"schema": "CONFORMED"
 }

session = Session.builder.configs(connection_parameters).create()

session.add_packages('snowflake-snowpark-python')

def insert_into_table(session: snowflake.snowpark.Session):


    session.sql('''
                create or replace TABLE SCM_PROD.CONFORMED.CUSTOMER_TABLE 
                (
                    CUSTOMER_ID     NUMBER(38,0),
                    DATE            VARCHAR(16777216),
                    SALUTATION      VARCHAR(5),
                    FIRST_NAME      VARCHAR(200),
                    LAST_NAME       VARCHAR(200),
                    DATE_INSERT     TIMESTAMP_NTZ(9)
                ); 
                ''').collect()
  

    session.sql('''
                INSERT INTO SCM_PROD.CONFORMED.CUSTOMER_TABLE
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
  
    return 'SUCCESS!'

## Register Stored Produre in Snowflake
## Add packages and data types


## Upload Stored Produre to Snowflake
session.sproc.register(
    func = insert_into_table
  , return_type = StringType()
  , input_types = []
  , is_permanent = True
  , name = 'SCM_PROD.CONFORMED.SNOWPARK_insert_into_table'
  , replace = True
  , stage_location = '@MY_STAGE'
)
