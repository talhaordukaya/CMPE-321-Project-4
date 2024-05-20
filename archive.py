import os
import time
import csv
import json

def log_operation(operation, status):
    with open('log.csv', 'a', newline='') as log_file:
        log_writer = csv.writer(log_file)
        log_writer.writerow([int(time.time()), operation, status])

def read_schema(type_name):
    with open(f"{type_name}.db", 'r') as type_file:
        schema = json.loads(type_file.readline().strip())
    return schema

def write_schema(type_name, schema):
    with open(f"{type_name}.db", 'w') as type_file:
        type_file.write(json.dumps(schema) + '\n')

def create_type(operation):
    _, _, type_name, num_fields, primary_key_order, *fields = operation.split()
    num_fields = int(num_fields)
    primary_key_order = int(primary_key_order)

    if os.path.exists(f"{type_name}.db"):
        log_operation(operation, "failure")
        return

    schema = {
        "num_fields": num_fields,
        "primary_key_order": primary_key_order,
        "fields": fields,
        "pages": []
    }
    write_schema(type_name, schema)
    log_operation(operation, "success")

def create_record(operation):
    parts = operation.split()
    type_name = parts[2]
    values = parts[3:]

    schema = read_schema(type_name)
    primary_key_index = schema["primary_key_order"] - 1
    primary_key = values[primary_key_index]

    for page in schema["pages"]:
        for record in page["records"]:
            if record[primary_key_index] == primary_key:
                log_operation(operation, "failure")
                return

    if not schema["pages"] or len(schema["pages"][-1]["records"]) >= 10:
        schema["pages"].append({"records": []})

    schema["pages"][-1]["records"].append(values)
    write_schema(type_name, schema)
    log_operation(operation, "success")

def delete_record(operation):
    _, _, type_name, primary_key = operation.split()

    schema = read_schema(type_name)
    primary_key_index = schema["primary_key_order"] - 1

    for page in schema["pages"]:
        for record in page["records"]:
            if record[primary_key_index] == primary_key:
                page["records"].remove(record)
                write_schema(type_name, schema)
                log_operation(operation, "success")
                return

    log_operation(operation, "failure")

def search_record(operation):
    _, _, type_name, primary_key = operation.split()

    schema = read_schema(type_name)
    primary_key_index = schema["primary_key_order"] - 1

    for page in schema["pages"]:
        for record in page["records"]:
            if record[primary_key_index] == primary_key:
                output = ' '.join(record)
                with open('output.txt', 'a') as output_file:
                    output_file.write(output + '\n')
                log_operation(operation, "success")
                return

    log_operation(operation, "failure")

def main(input_file_path):
    with open(input_file_path, 'r') as input_file:
        operations = input_file.readlines()

    for operation in operations:
        operation = operation.strip()
        if operation.startswith("create type"):
            create_type(operation)
        elif operation.startswith("create record"):
            create_record(operation)
        elif operation.startswith("delete record"):
            delete_record(operation)
        elif operation.startswith("search record"):
            search_record(operation)

if __name__ == "__main__":
    import sys
    input_file_path = sys.argv[1]
    main(input_file_path)
