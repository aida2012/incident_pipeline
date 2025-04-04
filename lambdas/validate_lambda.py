import boto3
import csv
import logging
from botocore.client import Config
import json
import io


logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

session = boto3.Session()

s3_client = session.client(
    's3',
    endpoint_url="http://host.docker.internal:4566",
    config=Config(s3={'addressing_style': 'path'})
)

MAPPING_FILE='config/columns_map.json'

def read_sample(file_obj, sample_size=1024):
    """Reads file and returns sample"""
    return file_obj["Body"].read(sample_size)


def is_utf8(sample, file_key):
    """Checks if the file is UTF-8 encoded"""
    try:
        sample.decode("utf-8")  
        return True
    except UnicodeDecodeError as e:
        log.error("The file %s is not UTF-8 encoded. %s", file_key, str(e))
        return False

def is_csv_format(file_key: str) -> bool:
    file_name = file_key.split('/')[-1]
    return file_name.lower().endswith('.csv')
    
def is_empty(sample, file_key):
    """Checks if the file is empty."""
    if not sample:
        log.error("The file %s is empty", file_key)
        return True
    return False

def move_file(bucket, file_key, destination_folder):
    """Moves the file to the correct destination (validated or error)."""
    destination_key = f"{destination_folder}/{file_key.split('/')[-1]}" 
    try:
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': file_key},
            Key=destination_key
        )
        s3_client.delete_object(Bucket=bucket, Key=file_key)
        log.info(f"File {file_key} moved to {destination_folder}.")
    except Exception as e:
        log.error("Failed to move file %s to %s: %s", file_key, destination_folder, str(e))


def is_valid(bucket, file_key):
    """Validates file and return True if it is valid; else, it returns False."""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=file_key)
        sample = read_sample(file_obj)
        return is_utf8(sample, file_key) and is_csv_format(file_key) and not is_empty(sample, file_key)
    except Exception as e:
        log.error("Error validating file %s: %s", file_key, str(e))
        return False

def get_mapping(bucket):
    """Obtains the column names mapping from S3."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=MAPPING_FILE)
        return json.loads(obj["Body"].read().decode('utf-8'))
    except Exception as e:
        log.error("Error cargando el mapeo de columnas: %s", str(e))
        return {}
    

def rename_columns(bucket, file_key):
    """Renames the columns of a CSV file based on the mapping and moves it to datasets/validated."""
    try:
        mapping = get_mapping(bucket)
        if not mapping:
            log.error("The column mappings could not be obtained.")
            move_file(bucket, file_key, "datasets/error")
            return
        
        obj = s3_client.get_object(Bucket=bucket, Key=file_key)
        csv_content = io.StringIO(obj["Body"].read().decode("utf-8"))
        
        reader = csv.reader(csv_content)
        output = io.StringIO()
        writer = csv.writer(output)

        headers = next(reader) 
        renamed_headers = [mapping.get(col, col) for col in headers]  
        writer.writerow(renamed_headers) 

        for row in reader:
            writer.writerow(row)

        s3_client.put_object(
            Bucket=bucket,
            Key=f"datasets/validated/{file_key.split('/')[-1]}",
            Body=output.getvalue()
        )
        
        log.info(f"File {file_key} stored in datasets/validated/.")
        s3_client.delete_object(Bucket=bucket, Key=file_key)
        

    except Exception as e:
        log.error("Error renaming columns in %s: %s", file_key, str(e))
        move_file(bucket, file_key, "datasets/error")


def lambda_handler(event, context):
    
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        file_key = record["s3"]["object"]["key"]

        if is_valid(bucket, file_key):
            rename_columns(bucket, file_key)
        else:
            move_file(bucket, file_key, "datasets/error")

        return {
            "statusCode": 200,
            "body": f"File {file_key} processed successfully."
        }

    except Exception as e:
        log.error("Error processing file %s: %s", file_key, str(e))
        try:
            move_file(bucket, file_key, "error/")
        except Exception as inner_e:
            log.error("Error moving file to error folder: %s", str(inner_e))
            return {
                "statusCode": 500,
                "body": f"Error moving file {file_key} to error folder: {str(inner_e)}"
            }

        return {
            "statusCode": 500,
            "body": f"Error processing file {file_key}: {str(e)}"
        }
