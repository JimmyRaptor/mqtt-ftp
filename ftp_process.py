import inspect
import json
import logging
from logging.config import fileConfig
import os
import time
import datetime
import cbor2
import ftplib
import psycopg2
import psycopg2.extras
from psycopg2 import pool
import configparser

from utils import CRC, normalize_timestamp

config = configparser.ConfigParser()
config.read(['ftp.ini', 'database.ini'])

# FTP Configuration
FTP_HOST = config['ftp']['host']
FTP_USER = config['ftp']['user']
FTP_PASS = config['ftp']['password']


# configure logging
fileConfig('logging.ini')
logger = logging.getLogger('ftpLogger')

# Load and parse the FTP and Database configurations

DEV_DATABASE_CONFIG = config['dev']
dev_conn_info = {key: value for key, value in DEV_DATABASE_CONFIG.items()}

PROD_DATABASE_CONFIG = config['prod']
prod_conn_info = {key: value for key, value in PROD_DATABASE_CONFIG.items()}

batch = []
batch_size = 30


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
            logger.info(f"Moved {filename} to archive folder successfully.")
        except Exception as e:
            logger.info(f"Error moving {filename} to archive folder: {e}")

# Process the file
def process_file(ftp, filename):
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
                if len(batch) >= batch_size:
                    # Attempt to insert the batch into the database
                    success = insert_batch(
                        batch, dev_conn_info, prod_conn_info)
                    if success:
                        batch = []  # Reset batch only if insertion was successful
                    else:
                        return  # Do not proceed with moving the file if insertion failed
    if len(batch) > 0:
        # Attempt to insert any remaining records in the batch
        success = insert_batch(batch, dev_conn_info, prod_conn_info)
        if not success:
            return  # Do not proceed with moving the file if insertion failed
        batch = []

    # Define the source and destination paths
    source_path = filename
    destination_path = f"archive/{filename}"

    # Move the file to the 'archive' folder if insertion was successful
    try:
        ftp.rename(source_path, destination_path)
        logger.info(f"Moved {filename} to archive folder successfully.")
    except Exception as e:
        logger.error(f"Error moving {filename} to archive folder: {e}")

    # Cleanup: remove the temporary local file
    os.remove(local_filename)

def get_caller_filename():
    return inspect.stack()[2].filename

def insert_batch(batch, dev_conn_info, prod_conn_info):
    dev_conn = None
    prod_conn = None
    try:
        dev_conn = psycopg2.connect(**dev_conn_info)
        prod_conn = psycopg2.connect(**prod_conn_info)
        with dev_conn, prod_conn:
            with dev_conn.cursor() as DevCursor, prod_conn.cursor() as ProdCursor:
                transformed_batch = []
                for data in batch:
                    if "ts" in data and "id" in data:
                        try:
                            created_timestamp = datetime.datetime.now().replace(microsecond=0)
                            adjusted_timestamp = normalize_timestamp(
                                data["ts"])
                            date_format = '%Y-%m-%d %H:%M:%S'
                            adjusted_timestamp = datetime.datetime.strptime(
                                adjusted_timestamp[:-3], date_format)
                        except Exception as e:
                            logger.error(
                                f"Error in normalize_timestamp (from {get_caller_filename()}): {e}"
                            )
                            continue

                        record = (
                            created_timestamp,
                            adjusted_timestamp,
                            json.dumps(data),
                            data["id"],
                        )
                        transformed_batch.append(record)
                    else:
                        logger.warning(
                            "Missing 'ts' in data, skipping record.")

                if transformed_batch:
                    query = """
                    INSERT INTO ts.ts_raw (created, time, data, device_id)
                    VALUES %s
                    """
                    psycopg2.extras.execute_values(
                        DevCursor, query, transformed_batch)
                    psycopg2.extras.execute_values(
                        ProdCursor, query, transformed_batch)
                    dev_conn.commit()
                    prod_conn.commit()
    except Exception as e:
        logger.error(f"Error inserting batch into database: {e}")
        if dev_conn:
            dev_conn.rollback()
        if prod_conn:
            prod_conn.rollback()
    finally:

        if dev_conn:
            dev_conn.close()
        if prod_conn:
            prod_conn.close()


# Main loop
def main():
    while True:
        try:
            ftp = connect_to_ftp()
            check_and_process_files(ftp)
            ftp.quit()

            time.sleep(20)
        except Exception as e:
            logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()
