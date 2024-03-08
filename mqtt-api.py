import os
from dotenv import load_dotenv
import logging
import paho.mqtt.client as mqtt
import cbor2
import requests
import asyncio
from database_utils import insert_data

headers = {"Content-Type": "application/json"}
buffer = []
BATCH_SIZE = 50
CRCFAILNUM = 0
INSERTNUMBER = 0
RECORDNUMBER = 0

def CRC16(a, crc):
    for x in range(8):
        if (a & 0x01) ^ (crc & 0x01):
            crc >>= 1
            crc ^= 0x8408
        else:
            crc >>= 1
        a >>= 1
    return crc


def CRC(packet, generate):
    crc = 0
    for x in range(len(packet)):
        crc = CRC16(packet[x], crc)
    if not generate:
        if crc == 0xF0B8:
            return True
        else:
            return False
    else:
        return (crc ^ 65535).to_bytes(2, "little")


# configure logging
logging.basicConfig(
    filename="mqtt-api.log",
    filemode="a",
    format="%(asctime)s - %(message)s",
    level=logging.INFO,
)

# Load environment variables
load_dotenv()

# Configuration
mqtt_url = "35.247.9.156"
mqtt_username = "test"
mqtt_password = "test"
post_url = "http://35.247.9.156:3001/mqttjson"


# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker")
    client.subscribe("/pk/telemetry/#")


def on_message(client, userdata, msg):
    global RECORDNUMBER
    global buffer
    if CRC(msg.payload, False):
        try:
            data = cbor2.loads(msg.payload)
            data = dict(data)
            data_id = msg.topic.split("/")[3]
            data["id"] = data_id
            requests.post(post_url, json=data, headers=headers)
            RECORDNUMBER += 1
            logging.info(f"RECORDNUMBER: {RECORDNUMBER}")
            buffer.append(data)
            print(len(buffer))
            if len(buffer) >= BATCH_SIZE:
                asyncio.run(handle_batch())
        except Exception as err:
            print(f"CBOR parsing error: {err}")
    else:
        global CRCFAILNUM
        CRCFAILNUM += 1
        logging.info(f"CRCFAILNUM: {CRCFAILNUM}")


async def handle_batch():
    global buffer
    global INSERTNUMBER
    await insert_data(buffer)
    # Clear the buffer after insertion to avoid re-inserting the same items
    buffer.clear()
    INSERTNUMBER += 1
    logging.info(f"INSERTNUMBER: {INSERTNUMBER}")


# Connect to the MQTT broker
async def connect_and_subscribe_to_mqtt():
    client = mqtt.Client()
    client.username_pw_set(username=mqtt_username, password=mqtt_password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(mqtt_url, 1883, 60)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, client.loop_forever)


if __name__ == "__main__":
    asyncio.run(connect_and_subscribe_to_mqtt())
