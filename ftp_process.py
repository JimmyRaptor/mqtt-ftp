import json
import logging
import logging.config
import os
import time

import cbor2
import ftplib
import psycopg2
import psycopg2.extras
from psycopg2 import pool

from utils import CRC, normalize_timestamp

FTP_HOST = "35.247.9.156"
FTP_USER = "raptorcore"
FTP_PASS = "rosCore!"


# configure logging
logging.basicConfig(
    filename="ftp-process.log",
    filemode="a",
    format="%(asctime)s - %(message)s",
    level=logging.INFO,
)

# Database configuration
DEV_DATABASE_CONFIG = {
    "user": "raptortech",
    "password": "raptortechAreCool1!",
    "database": "mqtt",
    "host": "10.162.0.2",
    "port": "5432",
}

PROD_DATABASE_CONFIG = {
    "user": "raptortech",
    "password": "raptortechAreCool1!",
    "database": "mqttjson",
    "host": "10.128.0.7",
    "port": "5432",
}

# Connect to the database
DevDB_connection = pool.SimpleConnectionPool(
    1,
    5,
    dbname=DEV_DATABASE_CONFIG["database"],
    user=DEV_DATABASE_CONFIG["user"],
    password=DEV_DATABASE_CONFIG["password"],
    host=DEV_DATABASE_CONFIG["host"],
    port=DEV_DATABASE_CONFIG["port"],
)

ProdDB_connection = pool.SimpleConnectionPool(
    1,
    5,
    dbname=PROD_DATABASE_CONFIG["database"],
    user=PROD_DATABASE_CONFIG["user"],
    password=PROD_DATABASE_CONFIG["password"],
    host=PROD_DATABASE_CONFIG["host"],
    port=PROD_DATABASE_CONFIG["port"],
)

fileNumber = 0
RecordNumber = 0
batch = []
batch_size = 50


# Connect to the FTP server
def connect_to_ftp():
    try:
        ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
        ftp.cwd("/")
        print("Successfully connected to FTP server.")
        return ftp
    except Exception as e:
        print(f"Failed to connect to FTP server: {e}")
        return None


def check_and_process_files(ftp):
    global fileNumber
    current_files = ftp.nlst()  # List of files in the current directory
    folder_names = [
        "cfg",
        "jake",
        "kyle",
        "test",
        "write",
        "archive",
    ]  # List of folder names to ignore

    for filename in current_files:
        if filename in folder_names or os.path.splitext(filename)[1]:
            continue  # Skip folders and files with extensions

        process_file(ftp, filename)
        # Define the source and destination paths
        source_path = filename
        destination_path = f"archive/{filename}"

        # Move the file to the 'archive' folder
        try:
            ftp.rename(source_path, destination_path)
            logging.info(f"Moved {filename} to archive folder successfully.")
        except Exception as e:
            logging.info(f"Error moving {filename} to archive folder: {e}")

# Process the file


def process_file(ftp, filename):
    global RecordNumber
    global batch
    local_filename = os.path.join("/tmp", filename)
    with open(local_filename, "wb") as local_file:
        ftp.retrbinary("RETR " + filename, local_file.write)

    # Read the file content
    with open(local_filename, "rb") as local_file:
        file_content = local_file.read()

    device_id = filename.split("_")[0]

    json_strings = file_content.split(b"xxx")
    for json_str in json_strings:
        if CRC(json_str, False):
            if len(json_str) >= 20:
                data = cbor2.loads(json_str)
                data = dict(data)
                data["id"] = device_id
                batch.append(data)
                if len(batch) >= 50:
                    # Insert the batch into the database
                    insert_batch(batch, DevDB_connection, ProdDB_connection)
    if len(batch) > 0:
        insert_batch(batch, DevDB_connection, ProdDB_connection)
        batch = []
    os.remove(local_filename)


def get_caller_filename():
    return inspect.stack()[2].filename


def insert_batch(batch, DevDB_connection, ProdDB_connection):
    """Inserts a batch of records into the PostgreSQL database."""
    try:
        with DevDB_connection.getconn() as dev_conn, ProdDB_connection.getconn() as prod_conn:
            DevCursor = dev_conn.cursor()
            ProdCursor = prod_conn.cursor()

            transformed_batch = []
            for data in batch:
                if "ts" in data and "id" in data:
                    try:
                        adjusted_timestamp = normalize_timestamp(data["ts"])
                    except Exception as e:
                        logging.error(
                            f"Error in normalize_timestamp (from {get_caller_filename()}): {e}"
                        )
                        continue

                    record = (
                        adjusted_timestamp,
                        json.dumps(data),
                        data["id"],
                    )
                    transformed_batch.append(record)
                else:
                    logging.warning("Missing 'ts' in data, skipping record.")

            if transformed_batch:
                query = "INSERT INTO mqttjson (time, data, device_id) VALUES %s;"

                psycopg2.extras.execute_values(
                    DevCursor, query, transformed_batch, template=None
                )

                psycopg2.extras.execute_values(
                    ProdCursor, query, transformed_batch, template=None
                )

                dev_conn.commit()
                prod_conn.commit()

                global RecordNumber
                RecordNumber += len(transformed_batch)
                logging.info(
                    f"Batch inserted into DevDB, record total: {RecordNumber}")
                logging.info(
                    f"Batch inserted into ProdDB, record total: {RecordNumber}")
            else:
                logging.warning("No valid data to insert.")

    except Exception as e:
        logging.error(f"Error inserting batch into database: {e}")


# Main loop
def main():
    while True:
        try:
            ftp = connect_to_ftp()
            check_and_process_files(ftp)
            ftp.quit()
            time.sleep(20)
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
