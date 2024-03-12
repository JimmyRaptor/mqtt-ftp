import asyncpg
import datetime
import json
import logging
import logging.config
import configparser
from utils import normalize_timestamp



config = configparser.ConfigParser()
config.read('database.ini')

DEV_DATABASE_CONFIG = {key: value for key, value in config['dev'].items()}
PROD_DATABASE_CONFIG = {key: value for key, value in config['prod'].items()}

logging.config.fileConfig('logging.ini')
logger = logging.getLogger('databaseLogger')

async def insert_data(data_list):
    try:
        dev_conn = await asyncpg.connect(**DEV_DATABASE_CONFIG)
        try:
            # Insert logic for DEV database
            await execute_insert(dev_conn, data_list)
            logger.info("Data successfully inserted into DEV database.")
        except Exception as dev_error:
            logger.exception(f"Could not upsert data into DEV database: {dev_error}")

        prod_conn = await asyncpg.connect(**PROD_DATABASE_CONFIG)
        try:
            # Insert logic for PROD database
            await execute_insert(prod_conn, data_list)
            logger.info("Data successfully inserted into PROD database.")
        except Exception as prod_error:
            logger.exception(f"Could not upsert data into PROD database: {prod_error}")

    finally:
        await dev_conn.close()
        await prod_conn.close()

async def execute_insert(conn, data_list):

    insert_query = """
        INSERT INTO ts.ts_raw (created, time, data, device_id)
        VALUES ($1, $2, $3, $4)
    """
    
    values = []
    for data in data_list:
        created_timestamp = datetime.datetime.now().replace(microsecond=0)
        if "ts" not in data:
            logger.error("Error: Missing 'ts' key in data item.")
            continue

        try:
            if "1" in data and int(data.get("1")) == 1: 
                adjusted_timestamp = datetime.datetime.now().replace(microsecond=0)
            else: 
                adjusted_timestamp = normalize_timestamp(data["ts"])
                date_format = '%Y-%m-%d %H:%M:%S'
                adjusted_timestamp = datetime.datetime.strptime(adjusted_timestamp[:-3], date_format)
            value_tuple = (
                created_timestamp,
                adjusted_timestamp,
                json.dumps(data),
                data.get("id", "default_id"),
            )
            values.append(value_tuple)
            
        except KeyError as e:
            logger.info(f"Error: Missing required key {e} in data item.")
            continue
        
    if not values:
        logger.info("No valid data items to insert.")
        return

    await conn.executemany(insert_query, values)
