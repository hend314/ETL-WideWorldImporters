import pandas as pd 
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pyodbc
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format= "%(asctime)s - %(message)s")

# table names 
tables = ['Sales.Customers', 'Sales.Invoices', 'Sales.Orders', 'Warehouse.StockItems','Warehouse.Colors',]

# Sqlserver connection
driver = 'ODBC Driver 17 for SQL Server'
server = 'localhost'
sqlsrv_db = 'WideWorldImporters'

sqlsever_con = f"DRIVER={driver};SERVER={server};DATABASE={sqlsrv_db};Trusted_Connection=yes"
conn_url = URL.create("mssql+pyodbc", query={"odbc_connect": sqlsever_con})
sqlsrv_engine = create_engine(conn_url)

# PostgreSQL  connection
uid = os.getenv("PG_UID")
pwd = os.getenv("PG_PWD")
host = os.getenv("PG_HOST")
pg_db = os.getenv("PG_DB")

pg_url = f'postgresql://{uid}:{pwd}@{host}:5432/{pg_db}'
pg_engine = create_engine(pg_url)

# Data extraction
def extract(table):
    try:
        schema_name = table.split(".")[0]
        tbl_name= table.split(".")[1]

        query = f"SELECT * FROM {schema_name}.{tbl_name}"
        df = pd.read_sql(query, sqlsrv_engine)

        logging.info(f"{len(df)} rows extracted")
        return df
    
    except Exception as e:
        logging.error(f"Data extract error: {e}")
    
# Data transformation
def transform(df, tbl_name):
    try:
        if tbl_name == "StockItems":
            df["ColorID"] = df["ColorID"].fillna(0)
            df["Brand"] = df["Brand"].fillna("Unknown")

        numeric_cols = df.select_dtypes(include=["number"]).columns
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())

        logging.info(f"Data transformed")
        return df
    
    except Exception as e:
        logging.error(f"Data transform error: {e}")

# Data loading 
def load(df, tbl_name):
    try:
        df.to_sql(tbl_name, con=pg_engine, if_exists='replace', index=False)
        logging.info(f"load {len(df)} rows in postgres")

    except Exception as e:
        logging.error(f"Data load error: {e}")

# Runs the ETL process    
def etl():
    for table in tables:
        df = extract(table)
        tbl_name = table.split(".")[1]
        df = transform(df, tbl_name)
        load(df, tbl_name)

if __name__ == "__main__":
    etl()