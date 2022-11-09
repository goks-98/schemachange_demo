CREATE OR REPLACE PROCEDURE SCM_PROD.CONFORMED.PY_PROCEDURE("FROM_TABLE" VARCHAR(16777216), "JOIN_TABLE" VARCHAR(16777216), "TO_TABLE" VARCHAR(16777216))
RETURNS VARCHAR(500)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS 
$$
def run(session, from_table, join_table, to_table):
    try:
        cust_table = session.sql(f"""INSERT INTO {to_table}
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
                        FROM {from_table} C
                        JOIN {join_table} D 
                        ON D_DATE_SK = C_FIRST_SHIPTO_DATE_SK
                        LIMIT 100""").collect();
                        
    except SnowparkSQLException as err:
        print("Error: ", err);
                    
    return cust_table, cust_table_2;
$$;