from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import pygeohash as pgh
import os

RESTAURANT_PATH = "restaurant_csv/"
OUTPUT_PATH = "output/enriched_restaurants.csv"
OPENCAGE_KEY = ""
OPENCAGE_URL = "https://api.opencagedata.com/geocode/v1/json"

def read_restaurants(**context):
    files = [os.path.join(RESTAURANT_PATH, f) for f in os.listdir(RESTAURANT_PATH) if f.endswith('.csv')]
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    df.to_json('/tmp/restaurants_df.json')

def fill_missing_latlng(**context):
    df = pd.read_json('/tmp/restaurants_df.json')
    def geocode(row):
        if pd.isnull(row['lat']) or pd.isnull(row['lng']):
            query = f"{row['city']}, {row['country']}"
            try:
                resp = requests.get(OPENCAGE_URL, params={"q": query, "key": OPENCAGE_KEY, "limit": 1, "no_annotations": 1}, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                if data["results"]:
                    geo = data["results"][0]["geometry"]
                    return pd.Series([geo["lat"], geo["lng"]])
            except:
                pass
            return pd.Series([0.0, 0.0])
        return pd.Series([row['lat'], row['lng']])
    df[['lat', 'lng']] = df.apply(geocode, axis=1)
    df.to_json('/tmp/restaurants_df_filled.json')

def add_geohash(**context):
    df = pd.read_json('/tmp/restaurants_df_filled.json')
    def geohash4(row):
        if pd.isnull(row['lat']) or pd.isnull(row['lng']):
            return None
        return pgh.encode(row['lat'], row['lng'], precision=4)
    df['geohash'] = df.apply(geohash4, axis=1)
    df.to_json('/tmp/restaurants_df_geohash.json')

def write_output(**context):
    df = pd.read_json('/tmp/restaurants_df_geohash.json')
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'restaurant_etl',
    default_args=default_args,
    schedule=None,
    catchup=False
)

read_task = PythonOperator(
    task_id='read_restaurants',
    python_callable=read_restaurants,
    dag=dag
)

fill_latlng_task = PythonOperator(
    task_id='fill_missing_latlng',
    python_callable=fill_missing_latlng,
    dag=dag
)

geohash_task = PythonOperator(
    task_id='add_geohash',
    python_callable=add_geohash,
    dag=dag
)

write_task = PythonOperator(
    task_id='write_output',
    python_callable=write_output,
    dag=dag
)

read_task >> fill_latlng_task >> geohash_task >> write_task
