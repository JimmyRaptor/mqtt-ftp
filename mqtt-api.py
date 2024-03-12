import logging
import logging.config
import paho.mqtt.client as mqtt
import cbor2
import asyncio
import configparser
import aiohttp
from functools import partial
from database_utils import insert_data
from utils import CRC


headers = {"Content-Type": "application/json"}
buffer = asyncio.Queue()
BATCH_SIZE = 50
CRC_FAIL_NUM = 0



# configure logging
logging.config.fileConfig('logging.ini')
logging = logging.getLogger("mqttLogger")



config = configparser.ConfigParser()
config.read('mqtt.ini')


mqtt_url = config['mqtt']['url']
mqtt_username = config['mqtt']['username']
mqtt_password = config['mqtt']['password']
post_url = config['http']['post_url']

async def http_post(session, url, data):
    async with session.post(url, json=data, headers=headers) as response:
        return await response.text()

def on_connect(client, userdata, flags, rc):
    client.subscribe("/pk/telemetry/#")


def on_message(client, userdata, msg, loop):
    asyncio.run_coroutine_threadsafe(handle_message(msg), loop)


async def handle_message(msg):
    global count
    if CRC(msg.payload, False):
        if msg.payload[0] == 0x78:
            msg.payload = msg.payload[1:]
        data = cbor2.loads(msg.payload)
        data = dict(data)
        data_id = msg.topic.split("/")[3]
        data["id"] = data_id
        async with aiohttp.ClientSession() as session:
            await http_post(session, post_url, data)
        await buffer.put(data)
        if buffer.qsize() >= BATCH_SIZE:
            await handle_batch()

async def handle_batch():
    global buffer
    items = []
    for _ in range(BATCH_SIZE):
        item = await buffer.get()
        items.append(item)
        buffer.task_done()  

    await insert_data(items)
    print(f"Batch insert successful: {len(items)} items inserted.")
    
async def connect_and_subscribe_to_mqtt():
    loop = asyncio.get_running_loop()
    client = mqtt.Client()
    client.username_pw_set(username=mqtt_username, password=mqtt_password)
    client.on_connect = on_connect
    client.on_message = lambda client, userdata, msg: asyncio.run_coroutine_threadsafe(handle_message(msg), loop)
    client.connect(mqtt_url, 1883, 60)
    client.loop_start()

async def main():
    await connect_and_subscribe_to_mqtt()
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Program stopped by KeyboardInterrupt")
