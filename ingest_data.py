#!/usr/bin/env python
# coding: utf-8

import ssl
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm 
import click
import urllib.request

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# df = pd.read_csv(
#     url,    
#     dtype=dtype,
#     parse_dates=parse_dates
# )


# # In[5]:


# df.head()


# # In[6]:


# len(df)


# # In[7]:


# In[11]:

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=11, type=int, help='Month of the data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    prefix='https://d37ci6vzurychx.cloudfront.net/trip-data'
    url=f'{prefix}/green_tripdata_{year}-{month:02d}.parquet'
    local_file = f'green_tripdata_{year}-{month:02d}.parquet'

    print("After url creation")
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    print("After engine creation")

    # Create unverified SSL context
    ssl_context = ssl._create_unverified_context()
    urllib.request.urlretrieve(url, local_file, context=ssl_context)
    print(f"Downloaded to {local_file}")

    print(f"Before read parquet {local_file}")
    df = pd.read_parquet(
        local_file,
        engine='pyarrow'
    )
    print("After read parquet")

    print("Before create table")
    # Create table schema (no data)
    df.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )
    print("Table created")

    # Insert data in chunks
    total_rows = len(df)
    for i in tqdm(range(0, total_rows, chunksize)):
        df_chunk = df.iloc[i:i+chunksize]
        
        # df_chunk.to_sql(
        #     name=target_table,
        #     con=engine,
        #     if_exists="append"
        #)

        print(f"Inserted: {len(df_chunk)} rows")

if __name__ == "__main__":
    run()