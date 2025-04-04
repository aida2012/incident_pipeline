import logging
import json
from botocore.exceptions import ClientError
import os

def set_log(name, log_level=logging.INFO):
 
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    log = logging.getLogger(name)

    if not log.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        log.addHandler(handler)

    log.setLevel(log_level)
    log.propagate = True  
    
    log.info("Log Level is set: %s", logging.getLevelName(log_level))
    return log


log=set_log(__name__,logging.INFO)
def load_config(path):
    try:
        CONFIG_PATH = os.path.join(os.path.dirname(__file__),path )
        with open(CONFIG_PATH, 'r') as file:
            config = json.load(file) 
        return config

    except Exception as e:
        log.error("There is an error in the configuration file: %s", e)
        raise

def get_secret(secret_name, session):
    try:
        client = session.client(
            "secretsmanager",
            endpoint_url="http://localhost:4566"  
        )

        response = client.get_secret_value(SecretId=secret_name)
        
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        else:
            log.warning("The secret %s does not contain a valid string", secret_name)
            return {}

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "ResourceNotFoundException":
            log.error("The secret %s does not exist", secret_name)
        elif error_code == "AccessDeniedException":
            log.error("You do not have permission to access the secret %s",secret_name)
        else:
            log.error(e)
        return {}