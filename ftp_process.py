import os
import time
import json
import logging
from logging.config import fileConfig
import datetime
import cbor2
import ftplib
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import text
from sqlalchemy.orm import declarative_base, sessionmaker
import configparser
from utils import CRC, normalize_timestamp 


config = configparser.ConfigParser()
config.read(['ftp.ini', 'database.ini'])


fileConfig('logging.ini')
logger = logging.getLogger('ftpLogger')

Base = declarative_base()




DEV_DATABASE_URL = f"postgresql://{config['dev']['user']}:{config['dev']['password']}@{config['dev']['host']}:{config['dev']['port']}/{config['dev']['database']}"
PROD_DATABASE_URL = f"postgresql://{config['prod']['user']}:{config['prod']['password']}@{config['prod']['host']}:{config['prod']['port']}/{config['prod']['database']}"


dev_engine = create_engine(DEV_DATABASE_URL, pool_size=10, max_overflow=20)
prod_engine = create_engine(PROD_DATABASE_URL, pool_size=10, max_overflow=20)

DevSession = sessionmaker(bind=dev_engine)
ProdSession = sessionmaker(bind=prod_engine)

FTP_HOST = config['ftp']['host']
FTP_USER = config['ftp']['user']
FTP_PASS = config['ftp']['password']


batch = []
batch_size = 30
called_count = 0

def connect_to_ftp():
    try:
        ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
        ftp.cwd("/")
        logger.info("Successfully connected to FTP server.")
        return ftp
    except Exception as e:
        logger.error(f"Failed to connect to FTP server: {e}")
        return None


def check_and_process_files(ftp):
    current_files = ftp.nlst()
    folder_names = ["cfg", "jake", "kyle", "test", "write", "archive"]

    for filename in current_files:
        if filename in folder_names or os.path.splitext(filename)[1]:
            continue

        with DevSession() as dev_session, ProdSession() as prod_session:
            process_file(ftp, filename, dev_session, prod_session)

        source_path = filename
        destination_path = f"archive/{filename}"
        try:
            ftp.rename(source_path, destination_path)
            logger.info(f"Moved {filename} to archive folder successfully.")
        except Exception as e:
            logger.error(f"Error moving {filename} to archive folder: {e}")

def extract_object_values(data):
    global called_count  
    result = {}
    for key, value in data.items():
        if isinstance(value, dict):
            called_count += 1
            logger.info("called extract function ",called_count)
            result[key] = value.values()
        else:
            result[key] = value
    
    return result

def process_file(ftp, filename, dev_session, prod_session):
    global batch
    local_filename = os.path.join("/tmp", filename)
    with open(local_filename, "wb") as local_file:
        ftp.retrbinary("RETR " + filename, local_file.write)

    with open(local_filename, "rb") as local_file:
        file_content = local_file.read()

    device_id = filename.split("_")[0]
    json_strings = file_content.split(b"xxx")

    for json_str in json_strings:
        if CRC(json_str, False):
            if len(json_str) >= 20:
                data = cbor2.loads(json_str)
                #data = dict(data)
                data = extract_object_values(data)
                data["id"] = device_id
                batch.append(data)
                if len(batch) >= batch_size:
                    process_and_insert_batch(batch, dev_session, prod_session)
                    batch = []

    if len(batch) > 0:
        process_and_insert_batch(batch, dev_session, prod_session)
        batch = []

    os.remove(local_filename)


def transform_data(batch):
    transformed_batch = []
    for data in batch:
        if "ts" in data and "id" in data:
            try:
                created_timestamp = datetime.datetime.now().replace(microsecond=0)
                adjusted_timestamp = normalize_timestamp(data["ts"])
                date_format = '%Y-%m-%d %H:%M:%S'
                adjusted_timestamp = datetime.datetime.strptime(
                    adjusted_timestamp[:-3], date_format)
                record = {
                    'created': created_timestamp,
                    'time': adjusted_timestamp,
                    'data': json.dumps(data),
                    'device_id': data["id"],
                }
                transformed_batch.append(record)
            except Exception as e:
                logger.error(f"Error in normalize_timestamp: {e}")
        else:
            logger.warning("Missing 'ts' in data, skipping record.")
    return transformed_batch


def insert_transformed_data(session, transformed_batch):
    start_time = time.time()  
    try:
        insert_query = text("""
            INSERT INTO ts.ts_raw (created, time, data, device_id)
            VALUES (:created, :time, :data, :device_id)
        """)
        
        for record in transformed_batch:
            session.execute(insert_query, {
                'created': record['created'],
                'time': record['time'],
                'data': record['data'],
                'device_id': record['device_id']
            })
        
        session.commit()
        end_time = time.time()  
        elapsed_time = end_time - start_time  
        logger.info(f"Batch inserted successfully. Execution time: {elapsed_time:.2f} seconds.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error inserting batch into database: {e}")


def process_and_insert_batch(batch, dev_session, prod_session):
    transformed_batch = transform_data(batch)
    insert_transformed_data(dev_session, transformed_batch)
    insert_transformed_data(prod_session, transformed_batch)


def main():
    while True:
        try:
            ftp = connect_to_ftp()
            if ftp:
                check_and_process_files(ftp)
                ftp.quit()
            time.sleep(20)
        except Exception as e:
            logger.error(f"Error in main loop: {e}")


if __name__ == "__main__":
    main()
