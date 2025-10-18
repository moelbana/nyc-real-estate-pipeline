import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

# local Database
local_db_url = os.getenv("LOCAL_DB_URL")
local_schema = 'mart'

# Render Postgres instance
cloud_db_url = os.getenv("CLOUD_DB_URL") 


local_engine = create_engine(local_db_url)
cloud_engine = create_engine(cloud_db_url)


tables_to_migrate = ['fct_property_sales', 'dim_property', 'dim_date'] 

print(f"Starting data migration from schema '{local_schema}'...")


for table in tables_to_migrate:
    print(f"Migrating table: {table}...")
    
    # Use read_sql_table to specify the schema to read from
    df = pd.read_sql_table(table, local_engine, schema=local_schema)

    df.to_sql(table, cloud_engine, if_exists='replace', index=False, schema='mart')
    
    print(f"Successfully migrated {table} with {len(df)} rows to schema 'mart'.")

print("\nData migration complete")