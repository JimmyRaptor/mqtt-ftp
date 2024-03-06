import asyncio
import asyncpg
import datetime
import json
import logging

DATABASE_CONFIG = {
    "user": "raptortech",
    "password": "raptortechAreCool1!",
    "database": "mqtt",
    "host": "localhost",
    "port": 5433,
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
        conn = await asyncpg.connect(**DATABASE_CONFIG)
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
        # Execute batch insert
        start_time = asyncio.get_event_loop().time()
        await conn.executemany(insert_query, values)
        end_time = asyncio.get_event_loop().time()
        duration = end_time - start_time
        print(f"Data inserted successfully in {duration} seconds")
    except Exception as error:
        print(f"Could not insert data: {error}")
    finally:
        await conn.close()
