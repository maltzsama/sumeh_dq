

def get_config(kind:str, *args, **kwargs):
    match kind:
        case "csv":
            return __read_csv_file(*args, **kwargs)
        case "json":
            return __read_json_file(*args, **kwargs)

def __read_csv_file(file_path: str, delimiter:str = ";") -> list:
    import csv
    from collections import namedtuple

    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=delimiter)
        headers = next(reader)
        Row = namedtuple('Row', headers)
        data = [Row(*row) for row in reader]
    return data

def __read_json_file(file_path: str):
    import json
    with open(file_path, mode='r', encoding='utf-8') as file:
        return json.load(file)
