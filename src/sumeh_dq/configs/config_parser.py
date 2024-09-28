#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import StringIO


def get_config(kind: str, file_path: str, delimiter: str = ";"):
    if file_path.startswith("s3://"):
        file_content = __read_s3_file(file_path)
    else:
        file_content = __read_local_file(file_path)

    match kind:
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
    """Parse s3://bucket/key and return bucket and key"""
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
    import ast
    from datetime import datetime

    parsed_data = []

    for row in data:
        # Parse individual fields
        parsed_row = {
            "field": (
                ast.literal_eval(row["field"])
                if isinstance(row["field"], str) and row["field"].startswith("[")
                else row["field"]
            ),
            "check_type": row["check_type"],
            "value": None if row["value"] == "NULL" else row["value"],
            "threshold": (
                None if row["threshold"] == "NULL" else float(row["threshold"])
            ),
            "execute": row["execute"].lower() == "true",
            "updated_at": datetime.strptime(row["updated_at"], "%d/%m/%Y"),
        }
        parsed_data.append(parsed_row)

    return parsed_data
