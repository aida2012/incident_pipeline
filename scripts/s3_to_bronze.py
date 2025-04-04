

import boto3
from io import StringIO
import pandas as pd
import os
from helpers.utils import set_log, load_config, get_secret
import logging
import psycopg2
from sqlalchemy import create_engine
import time
from datetime import datetime

start_time = time.time()

# Set logs level
log=set_log(__name__,logging.INFO)

CONFIG_PATH='../config/config.json'

# Create session for localstack
session = boto3.Session(profile_name="localstack")

def generate_profiles_yml(dbt_host, dbt_user, dbt_password, dbt_dbname, dbt_schema):
    profiles_yml_content = f"""
default:
  target: dev
  outputs:
    dev:
      type: postgres
      host: {dbt_host}
      user: {dbt_user}
      password: {dbt_password}
      port: 5432
      dbname: {dbt_dbname}
      schema: {dbt_schema}
    """
    
    # Create .dbt directory if it does not exist
    os.makedirs(os.path.expanduser('~/.dbt'), exist_ok=True)
    
    # Create profiles.yml for dbt
    with open(os.path.expanduser('~/.dbt/profiles.yml'), 'w') as file:
        file.write(profiles_yml_content)

# Load configuration data
config_data = load_config(CONFIG_PATH)
AWS_SECRET = config_data['AWS']['secret_name']
AWS_REGION = config_data['AWS']['region']
AWS_BUCKET = config_data['AWS']['bucket']
AWS_PREFIX = config_data['AWS']['prefix']
FILES = {f['filename']:f['br_table'] for f in config_data['files']}
DB_SECRET = config_data['postgres']['secret_name']
DB_HOST = config_data['postgres']['host']
DB_PORT = config_data['postgres']['port']
DB_NAME = config_data['postgres']['database']

# Load aws and postgres secrets
AWSsecrets = get_secret(AWS_SECRET,session)
if AWSsecrets:
    # AWS credentials
    aws_access_key_id = AWSsecrets['aws_access_key_id']
    aws_secret_access_key = AWSsecrets['aws_secret_access_key']

DBsecrets = get_secret(DB_SECRET,session)
if DBsecrets:
    # PostgreSQL credentials
    DB_USER = DBsecrets['postgres_user']
    DB_PASS = DBsecrets['postgres_password']
else:
    log.info("Issues while reading DB secrets")

# Connecto to postgres
try:
    conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
    )

    log.info("Postgres connection successful")

    # Create sqlalchemy engine to write df to postgres
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?client_encoding=utf8')

    # generate profiles.yml for dbt
    generate_profiles_yml(DB_HOST,DB_USER,DB_PASS,DB_NAME,config_data['dbt']['validation_schema'])
except Exception as e:
    log.error("There was an error connecting to Postgres: %s",e)
    raise


s3_client = session.client('s3',endpoint_url="http://localhost:4566")

# Process each file
for file in FILES:
    try:
        log.info("Loading %s to Bronze",file)
        file_key = f"{AWS_PREFIX}{file}"
        log.info("AWS_BUCKET: %s, file_key: %s",AWS_BUCKET,file_key)

        # Get csv from S3 bucket
        response = s3_client.get_object(Bucket=AWS_BUCKET, Key=file_key)
        
        df = pd.read_csv(StringIO(response["Body"].read().decode("utf-8")), low_memory=False, dtype=str)
        
        columns_fix = [
            "floor_of_fire_origin",
            "num_floors_min_damage",
            "num_floors_significant_damage",
            "num_floors_heavy_damage",
            "num_floors_extreme_damage",
            "num_sprinkler_heads_operating"]
        
        for col in columns_fix:
            df[col] = df[col].str.replace(r'^(\d+)\.0$', r'\1', regex=True)
        
        timestamp=pd.to_datetime('now')
        df['insert_timestamp']=timestamp
        
    except Exception as e:
        log.error(e)
        raise

    # Load bronze tables - Postgres
    table_name = FILES[file]
    schema = 'bronze'
    try:
        output = StringIO()
        
        df.to_csv(output, sep='\t', index=False, header=False) 
        output.seek(0)

        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("SET search_path TO bronze;")
        
        cursor.copy_from(output, f'{table_name}', sep='\t', null='')

        conn.commit()

        cursor.execute(f"SELECT MAX(data_as_of) FROM bronze.{table_name}")
        last_data_as_of = cursor.fetchone()[0]

        # Update load_tracker table
        cursor.execute("""
                INSERT INTO incidents.metadata.load_tracker (model_name, last_data_as_of, prev_data_as_of)
                VALUES (%s, %s, NULL)
                ON CONFLICT (model_name)
                DO UPDATE SET 
                    prev_data_as_of = load_tracker.last_data_as_of,
                    last_data_as_of = EXCLUDED.last_data_as_of
            """, ('fire_incidents', last_data_as_of))

        conn.commit()  
        cursor.close()

        log.info("The table {} was successfully loaded.".format(table_name))

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        source_key = f"datasets/validated/{file}"
        destination_key = f"datasets/processed/{file}_{timestamp}"

        s3_client.copy_object(
        Bucket=AWS_BUCKET,
        CopySource=f"{AWS_BUCKET}/{source_key}",
        Key=destination_key
        )

        s3_client.delete_object(Bucket=AWS_BUCKET, Key=source_key)
        log.info(f"File {file} moved from validated/ to processed/")        
        
    except Exception as e:
        log.error("There was an error writing to Postgres: %s",e)
        raise
conn.close()
end_time = time.time() 
elapsed_time = end_time - start_time

log.info(f"Tiempo total de ejecución: {elapsed_time:.2f} segundos")
log.info("Connection closed")
