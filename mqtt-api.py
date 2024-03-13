import asyncio
import aiohttp
import cbor2
import configparser
import logging.config
import paho.mqtt.client as mqtt
from database_utils import insert_data
from utils import CRC


buffer = asyncio.Queue()
BATCH_SIZE = 10


logging.config.fileConfig('logging.ini')
logger = logging.getLogger("mqttLogger")


config = configparser.ConfigParser()
config.read('mqtt.ini')
mqtt_url = config['mqtt']['url']
mqtt_username = config['mqtt']['username']
mqtt_password = config['mqtt']['password']
post_url = config['http']['post_url']

headers = {"Content-Type": "application/json"}
session = None

async def http_post_and_log(url, data):
    global session
    try:
        async with session.post(url, json=data, headers=headers) as response:
            response_text = await response.text()
            logger.info(f"HTTP POST Response for {data['id']}: {response_text}")
    except Exception as e:
        logger.error(f"HTTP POST Request failed for {data['id']}: {e}")

def on_connect(client, userdata, flags, rc):
    logger.info("Connected with result code "+str(rc))
    client.subscribe("/pk/telemetry/#")

def on_message(client, userdata, msg, loop):
    asyncio.run_coroutine_threadsafe(handle_message(msg), loop)

async def handle_message(msg):
    if CRC(msg.payload, False):
        if msg.payload[0] == 0x78:
            msg.payload = msg.payload[1:]
        data = cbor2.loads(msg.payload)
        data = dict(data)
        data_id = msg.topic.split("/")[3]
        data["id"] = data_id
        asyncio.create_task(http_post_and_log(post_url, data))
        await buffer.put(data)
        if buffer.qsize() >= BATCH_SIZE:
            await handle_batch()

async def handle_batch():
    items = []
    for _ in range(BATCH_SIZE):
        item = await buffer.get()
        items.append(item)
        buffer.task_done()
    await insert_data(items)
    logger.info(f"Batch insert successful: {len(items)} items inserted.")

async def connect_and_subscribe_to_mqtt():
    loop = asyncio.get_running_loop()
    client = mqtt.Client()
    client.username_pw_set(username=mqtt_username, password=mqtt_password)
    client.on_connect = on_connect
    client.on_message = lambda client, userdata, msg: on_message(client, userdata, msg, loop)
    client.connect(mqtt_url, 1883, 60)
    client.loop_start()

async def main():
    global session
    session = aiohttp.ClientSession()
    await connect_and_subscribe_to_mqtt()
    await asyncio.Event().wait()
    await session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program stopped by KeyboardInterrupt")

