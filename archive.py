import os
import time
import csv
import json

PAGE_SIZE = 10 # Maximum number of records per page
LOG_FILE = 'log.csv'
OUTPUT_FILE = 'output.txt'

# Function to log operations with their status
def log_operation(operation, status):
    with open(LOG_FILE, 'a', newline='') as log_file:
        log_writer = csv.writer(log_file)
        log_writer.writerow([int(time.time()), operation, status])

# Function to read the schema header from a type file
def read_schema_header(type_name):
    try:
        with open(f"{type_name}.db", 'r') as type_file:
            header = json.loads(type_file.readline().strip())
        return header
    except FileNotFoundError:
        return None

# Function to read a specific page from a type file
def read_page(type_name, page_index):
    try:
        with open(f"{type_name}.db", 'r') as type_file:
            type_file.readline()  # Skip header
            for _ in range(page_index):
                json.loads(type_file.readline().strip())
            page = json.loads(type_file.readline().strip())
        return page
    except (FileNotFoundError, IndexError):
        return None

# Function to write the schema header to a type file
def write_schema_header(type_name, header):
    with open(f"{type_name}.db", 'w') as type_file:
        type_file.write(json.dumps(header) + '\n')
        for page in header['pages']:
            type_file.write(json.dumps(page) + '\n')

# Function to append a new page to a type file
def append_page(type_name, page):
    with open(f"{type_name}.db", 'a') as type_file:
        type_file.write(json.dumps(page) + '\n')

# Function to handle the creation of a new type
def create_type(operation):
    _, _, type_name, num_fields, primary_key_order, *fields = operation.split()
    num_fields = int(num_fields)
    primary_key_order = int(primary_key_order)

    # Check if the type already exists
    if os.path.exists(f"{type_name}.db"):
        log_operation(operation, "failure")
        return

    # Create the schema header for the new type
    header = {
        "num_fields": num_fields,
        "primary_key_order": primary_key_order,
        "fields": fields,
        "pages": []
    }
    write_schema_header(type_name, header)
    log_operation(operation, "success")

# Function to handle the creation of a new record
def create_record(operation):
    parts = operation.split()
    type_name = parts[2]
    values = parts[3:]

    header = read_schema_header(type_name)
    if header is None:
        log_operation(operation, "failure")
        return

    primary_key_index = header["primary_key_order"] - 1
    primary_key = values[primary_key_index]

    # Check for duplicate primary key in existing records
    for i in range(len(header['pages'])):
        page = read_page(type_name, i)
        if page is not None:
            for record in page["records"]:
                if record[primary_key_index] == primary_key:
                    log_operation(operation, "failure")
                    return

    # Add a new page if the last page is full
    if not header["pages"] or len(header["pages"][-1]["records"]) >= PAGE_SIZE:
        new_page = {"page_number": len(header["pages"]), "num_records": 0, "records": []}
        header["pages"].append(new_page)
        append_page(type_name, new_page)

    # Add the new record to the last page
    page = read_page(type_name, len(header['pages']) - 1)
    if page is not None:
        page["records"].append(values)
        page["num_records"] += 1
        header['pages'][-1] = page # Update the header with the new page details
        write_schema_header(type_name, header) # Rewrite the header to include the new page
        log_operation(operation, "success")
    else:
        log_operation(operation, "failure")

# Function to handle the deletion of a record
def delete_record(operation):
    _, _, type_name, primary_key = operation.split()

    header = read_schema_header(type_name)
    if header is None:
        log_operation(operation, "failure")
        return

    primary_key_index = header["primary_key_order"] - 1

    # Search and delete the record with the given primary key
    for i in range(len(header['pages'])):
        page = read_page(type_name, i)
        if page is not None:
            for record in page["records"]:
                if record[primary_key_index] == primary_key:
                    page["records"].remove(record)
                    page["num_records"] -= 1
                    header['pages'][i] = page
                    write_schema_header(type_name, header)
                    log_operation(operation, "success")
                    return

    log_operation(operation, "failure")

# Function to handle the search for a record by primary key
def search_record(operation):
    _, _, type_name, primary_key = operation.split()

    header = read_schema_header(type_name)
    if header is None:
        log_operation(operation, "failure")
        return

    primary_key_index = header["primary_key_order"] - 1

    # Search for the record with the given primary key
    for i in range(len(header['pages'])):
        page = read_page(type_name, i)
        if page is not None:
            for record in page["records"]:
                if record[primary_key_index] == primary_key:
                    output = ' '.join(record)
                    with open(OUTPUT_FILE, 'a') as output_file:
                        output_file.write(output + '\n')
                    log_operation(operation, "success")
                    return

    log_operation(operation, "failure")

# Main function to process the input file and execute operations
def main(input_file_path):
    # Clear the output file at the start of each run
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

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
