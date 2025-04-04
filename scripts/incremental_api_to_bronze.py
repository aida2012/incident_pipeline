import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from helpers.utils import set_log, load_config, get_secret
import logging
import boto3


log=set_log(__name__,logging.INFO)
session = boto3.Session(profile_name="localstack")
dataset_id = 'wr8u-xric'
base_url = f"https://data.sfgov.org/resource/{dataset_id}.json"
model_name = 'fire_incidents'  


CONFIG_PATH='../config/config.json'

def geojson_to_wkt(p):
    if isinstance(p, dict) and 'coordinates' in p:
        lon, lat = p['coordinates']
        return f"POINT({lon} {lat})"
    return None

# Load configuration data
config_data = load_config(CONFIG_PATH)
DB_SECRET = config_data['postgres']['secret_name']
DB_HOST = config_data['postgres']['host']
DB_PORT = config_data['postgres']['port']
DB_NAME = config_data['postgres']['database']

DBsecrets = get_secret(DB_SECRET,session)
if DBsecrets:
    # PostgreSQL credentials
    DB_USER = DBsecrets['postgres_user']
    DB_PASS = DBsecrets['postgres_password']
else:
    log.info("Issues while reading DB secrets")

engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?client_encoding=utf8')

# Obtain last date from metadata.load_tracker
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT last_data_as_of FROM incidents.metadata.load_tracker
        WHERE model_name = :model
    """), {"model": model_name})
    last_data_as_of = result.scalar()

if last_data_as_of is None:
    raise ValueError("There is no previous date for this model")

if isinstance(last_data_as_of, str):
    last_data_as_of = datetime.strptime(last_data_as_of, "%Y/%m/%d %I:%M:%S %p")

date_api = last_data_as_of.strftime('%Y-%m-%dT%H:%M:%S')
query = f"?$where=data_as_of > '{date_api}'"
url = base_url + query

# Request
response = requests.get(url)
if response.status_code != 200:
    raise Exception(f"API error: {response.status_code}")

data = response.json()
df_str = pd.DataFrame(data).astype(str).replace("nan", None)
if df_str.empty:
    log.info("No new data to insert.")
else:
    df_str['point'] = df_str['point'].apply(geojson_to_wkt)

    # Load bronze
    df_str.to_sql("br_fire_incidents", engine,schema="bronze", if_exists='append', index=False)
    log.info("br_fire_incidents loaded")

    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT MAX(data_as_of) 
            FROM bronze.{"br_fire_incidents"}
        """))
        max_data_as_of = result.scalar()

    # Insert or update load_tracker
    if max_data_as_of:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO incidents.metadata.load_tracker (model_name, last_data_as_of, prev_data_as_of)
                VALUES (:model, :last_ts, NULL)
                ON CONFLICT (model_name)
                DO UPDATE SET 
                    prev_data_as_of = load_tracker.last_data_as_of,
                    last_data_as_of = EXCLUDED.last_data_as_of
            """), {
                "model": "fire_incidents",
                "last_ts": max_data_as_of
            })
        log.info ("Load tracker updated")
    else:
        log.info("No data found in Bronze.")
