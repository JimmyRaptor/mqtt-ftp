import asyncio
import asyncpg
import datetime
import json
import logging

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

# Configure logging
logging.basicConfig(
    filename="insert-log.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


# Modify the timestamp_to_datetime_plus_30_years_in_seconds function to handle out-of-bound cases
def timestamp_to_datetime_plus_30_years_in_seconds(timestamp):
    # Convert timestamp to datetime
    date = datetime.datetime.fromtimestamp(timestamp)
    current_year = datetime.datetime.now().year

    # If the year is not this year, add 30 years
    if date.year != current_year:
        new_year = date.year + 30
        # Make sure the new year does not exceed the current year after adding 30 years
        if new_year > current_year:
            date = datetime.datetime.now()
        else:
            date = date.replace(year=new_year)
    # Return the date object
    return date


# Pass the connection pool as a parameter to the insert_data function
async def insert_data(data_list):
    try:
        dev_conn = await asyncpg.connect(**DEV_DATABASE_CONFIG)
        prod_conn = await asyncpg.connect(**PROD_DATABASE_CONFIG)
        insert_query = """
                INSERT INTO MQTTJSON (time, data, device_id) VALUES ($1, $2, $3)
            """
        # Prepare data for batch insertion
        values = []
        for data in data_list:
            if "ts" not in data:
                logging.error("Error: Missing 'ts' key in data item.")
                continue
            try:
                adjusted_timestamp = timestamp_to_datetime_plus_30_years_in_seconds(
                    data["ts"]
                )
                value_tuple = (
                    adjusted_timestamp,
                    json.dumps(data),
                    data.get("id", "default_id"),
                )
                values.append(value_tuple)
                
            except KeyError as e:
                print(f"Error: Missing required key {e} in data item.")
                continue
        if not values:
            print("No valid data items to insert.")
            return
        
        # Execute batch insert for DEV database
        start_time_dev = asyncio.get_event_loop().time()
        await dev_conn.executemany(insert_query, values)
        end_time_dev = asyncio.get_event_loop().time()

        # Execute batch insert for PROD database
        start_time_prod = asyncio.get_event_loop().time()
        await prod_conn.executemany(insert_query, values)
        end_time_prod = asyncio.get_event_loop().time()

        # Calculate duration for both databases
        duration_dev = end_time_dev - start_time_dev
        duration_prod = end_time_prod - start_time_prod
        logging.info(f"Data inserted successfully in DEV database in {duration_dev} seconds")
        logging.info(f"Data inserted successfully in PROD database in {duration_prod} seconds")
    except Exception as error:
        logging.info(f"Could not insert data: {error}")
    finally:
        await dev_conn.close()
        await prod_conn.close()
