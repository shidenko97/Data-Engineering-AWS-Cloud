import json
import logging
from typing import Dict

import boto3
from botocore import client
from pg import DB

SECRET_NAME = "redshift"
REGION_NAME = "eu-west-1"


def get_client(
        *, boto3_session: boto3.session.Session, service_name: str
) -> client.BaseClient:
    logging.info(f"Creating boto3 client [service_name={service_name}]")

    boto3_client = boto3_session.client(
        service_name=service_name, region_name=REGION_NAME
    )

    logging.info(f"boto3 client created [service_name={service_name}]")

    return boto3_client


def get_secret_values(
        *, boto3_session: boto3.session.Session
) -> Dict[str, str]:
    boto3_client = get_client(
        boto3_session=boto3_session, service_name="secretsmanager"
    )

    logging.info("Fetching secret values")

    secrets = boto3_client.get_secret_value(SecretId=SECRET_NAME)

    logging.info("Secret values fetched")

    return json.loads(secrets["SecretString"])


def run_db_query(
        *,
        database: str,
        host: str,
        port: int,
        username: str,
        password: str,
        query: str
) -> None:
    logging.info("Initializing DB connection")
    logging.info(
        f"DB params dbname={database}, host={host}, "
        f"port={port}, user={username}"
    )
    logging.info(f"query={query}")

    db = DB(
        dbname=database,
        host=host,
        port=port,
        user=username,
        passwd=password
    )

    logging.info("DB connection initialized")

    logging.info("Executing query")

    db.query(query)

    logging.info("Query executed")


def setup_logger() -> None:
    log_handler = logging.StreamHandler()
    log_formatter = logging.Formatter(fmt="%(asctime)s %(message)s")

    log_handler.setLevel(logging.INFO)
    log_handler.setFormatter(log_formatter)

    log = logging.getLogger()
    log.addHandler(log_handler)
    log.setLevel(logging.INFO)


if __name__ == "__main__":
    setup_logger()

    session = boto3.session.Session()

    credentials = get_secret_values(boto3_session=session)

    merge_query = """
        BEGIN;

            -- Copy hourly data from s3 to staging table
            COPY mysql_dwh_staging.order_payments FROM 's3://mysql-dwh-serhii/order_payments/current/order_payments.csv'
                IAM_ROLE 'arn:aws:iam::589464876810:role/Redshift'
                CSV QUOTE '\"' DELIMITER ','
                ACCEPTINVCHARS;
            
            -- Delete records from main table using staging
            DELETE
            FROM mysql_dwh.order_payments
                USING
                    mysql_dwh_staging.order_payments
            WHERE mysql_dwh.order_payments.order_id = mysql_dwh_staging.order_payments.order_id;
            
            -- Insert all staging data to main table
            INSERT INTO mysql_dwh.order_payments
            SELECT *
            FROM mysql_dwh_staging.order_payments;
            
            -- Truncate staging table
            TRUNCATE TABLE mysql_dwh_staging.order_payments;

        END;
    """

    run_db_query(
        database="ecommerce",
        host=credentials["host"],
        port=5439,
        username=credentials["username"],
        password=credentials["password"],
        query=merge_query
    )
