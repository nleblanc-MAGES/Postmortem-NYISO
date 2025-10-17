import pandas as pd
import os
import sqlparse
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

def split_sql_queries(queryText):
    queries_without_comments = sqlparse.format(queryText, strip_comments=True)
    queries = sqlparse.split(queries_without_comments)
    return [q for q in queries if not q.startswith('//')]

def establishconnection(warehouse,database,schema):
    username = os.environ['MAG_SNOWFLAKE_USERNAME']
    pathToKey = os.environ['MAG_SNOWFLAKE_PRIVATE_KEY_PATH']
    passphrase = os.environ['MAG_SNOWFLAKE_PASSPHRASE']
    
    with open(pathToKey, "rb") as key:
        p_key= serialization.load_pem_private_key(
            key.read(),
            password=passphrase.encode(),
            backend=default_backend()
        )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    
    conn = snowflake.connector.connect(
        user=username,
        private_key=pkb,
        account='mag.east-us-2.azure',
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    return(conn)

def executeQueryNatif(query_data,conn):
    statements_to_execute = split_sql_queries(query_data)
    cursor = conn.cursor()
    
    try:
        for statement in statements_to_execute:
            cursor.execute(statement)
        dataframe = cursor.fetch_pandas_all()
    finally:
        cursor.close()
    return dataframe

