#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import StringIO
import pandas as pd

def get_config(source_type: str, source: str, delimiter: str = ";", database_config: dict = None):
    if source.startswith("s3://"):
        file_content = __read_s3_file(source)
    elif source_type in ["mysql", "postgresql", "bigquery"]:
        if database_config is None:
            raise ValueError("database_config must be provided for database sources.")
        data = __read_database(source_type, database_config)
        return __parse_data(data)
    else:
        file_content = __read_local_file(source)

    match source_type:
        case "csv":
            return __read_csv_file(file_content=file_content, delimiter=delimiter)
        case "json":
            return __read_json_file(file_content=file_content)


def __read_s3_file(s3_path: str):
    try:
        import boto3
    except ImportError:
        raise ImportError(
            "boto3 is required to read from S3. Install it using 'pip install boto3'."
        )

    s3 = boto3.client("s3")
    bucket, key = __parse_s3_path(s3_path)
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")

def __parse_s3_path(s3_path: str):
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]  # remove s3://
    bucket, key = s3_path.split("/", 1)
    return bucket, key

def __read_local_file(file_path: str):
    with open(file_path, mode="r", encoding="utf-8") as file:
        return file.read()

def __read_csv_file(file_content: str, delimiter: str = ";") -> list:
    import csv

    reader = csv.DictReader(StringIO(file_content), delimiter=delimiter)
    data = [dict(row) for row in reader]
    return __parse_data(data)

def __read_json_file(file_content: str):
    import json

    return __parse_data(json.loads(file_content))

def __parse_data(data: list[dict]) -> list[dict]:
    from dateutil import parser

    parsed_data = []

    for row in data:
        parsed_row = {
            "field": (
                row["field"].strip("[]").split(",") if "[" in row["field"] else row["field"]
            ),
            "check_type": row["check_type"],
            "value": None if row["value"] == "NULL" else row["value"],
            "threshold": (
                None if row["threshold"] == "NULL" else float(row["threshold"])
            ),
            "execute": (
                row["execute"].lower() == "true" if isinstance(row["execute"], str) else row["execute"] is True
            ),
            "updated_at": parser.parse(row["updated_at"]),
        }
        parsed_data.append(parsed_row)

    return parsed_data

def __read_database(source_type: str, database_config: dict) -> list[dict]:
    if 'schema' not in database_config or 'table' not in database_config:
        raise ValueError("database_config must include 'schema' and 'table'.")

    table = database_config['table']
    schema = database_config['schema']

    if source_type == "mysql":
        try:
            import mysql.connector
            import pandas as pd
        except ImportError:
            raise ImportError(
                "mysql-connector-python is required to use MySQL as a source. "
                "You can install using 'pip install sumeh_dq[mysql]'"
                "OR"
                "Install it using 'pip install mysql-connector-python'."
                "You will need pandas too"
                "Install it using 'pip install pandas'"
            )

        connection = mysql.connector.connect(**{k: v for k, v in database_config.items() if k not in ['schema', 'table']})
        query = f"SELECT * FROM {schema}.{table}"  # Specify your query
        data = pd.read_sql(query, connection)
        connection.close()
    elif source_type == "postgresql":
        try:
            import pandas as pd
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is required to use Postgresql as a source. "
                "You can install using 'pip install sumeh_dq[postgresql]'"
                "OR"
                "Install it using 'pip install psycopg2'."
                "OR"
                "Install it using 'pip install psycopg2-binary'."
                "You will need pandas too"
                "Install it using 'pip install pandas'"
            )


        connection = psycopg2.connect(**{k: v for k, v in database_config.items() if k not in ['schema', 'table']})
        query = f"SELECT * FROM {schema}.{table}"  # Specify your query
        data = pd.read_sql(query, connection)
        connection.close()
    elif source_type == "bigquery":
        try:
            from google.cloud import bigquery
        except ImportError:
            raise ImportError(
                "google-cloud-bigquery is required to use BigQuery as a source. "
                "Install it using 'pip install sumeh_dq[bigquery]'."
                "Install it using 'pip install google-cloud-bigquery'."
            )

        client = bigquery.Client()
        query = f"SELECT * FROM `{schema}.{table}`"  # Specify your query
        data = client.query(query).to_dataframe()
    else:
        raise ValueError(f"Unsupported source_type: {source_type}")

    return data.to_dict(orient="records")  # Convert DataFrame to a list of dictionaries
