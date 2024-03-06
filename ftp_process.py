import ftplib
import os
import time
import paho.mqtt.client as mqtt


FTP_HOST = "localhost"
FTP_USER = "raptorcore"
FTP_PASS = "rosCore!"
FTP_DIR = "/"

# MQTT Broker settings
MQTT_BROKER = ""
MQTT_USER = "test"
MQTT_PASS = "test"
BASE_MQTT_TOPIC = "/pk/telemetry/"  # Base topic for MQTT messages

# Local file for tracking processed files
PROCESSED_FILES_TRACKER = "processed_files.txt"

# Log file
LOG_FILE = "ftp_log.txt"


# Connect to the FTP server
def connect_to_ftp():
    ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
    ftp.cwd("/")  # Change to root directory or a specific work directory
    return ftp


# Connect to the MQTT broker
def connect_to_mqtt():
    client = mqtt.Client()  # Create a new MQTT client instance
    client.username_pw_set(MQTT_USER, MQTT_PASS)  # Set username and password
    client.connect(MQTT_BROKER)  # Connect to the MQTT broker
    return client


# Check for new files and process them
def check_and_process_files(ftp, mqtt_client):
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
        if filename and not os.path.splitext(filename)[1]:
            print(f"Processing new file: {filename}")
            process_file(ftp, mqtt_client, filename)
        # Define the source and destination paths
        source_path = filename
        destination_path = f"archive/{filename}"

        # Move the file to the 'archive' folder
        try:
            ftp.rename(source_path, destination_path)
            print(f"Moved {filename} to archive folder successfully.")
        except Exception as e:
            print(f"Error moving {filename} to archive folder: {e}")


# Process the file
def process_file(ftp, mqtt_client, filename):
    mac_address = filename.split("_")[0]
    mqtt_topic = f"/pk/telemetry/{mac_address}/"
    local_filename = os.path.join("/tmp", filename)
    with open(local_filename, "wb") as local_file:
        ftp.retrbinary("RETR " + filename, local_file.write)

    # Read the file content
    with open(local_filename, "rb") as local_file:
        file_content = local_file.read()

    json_strings = file_content.split(b"xxx")
    for json_str in json_strings:
        if len(json_str) >= 20:
            try:
                mqtt_client.publish(
                    mqtt_topic, json_str
                )  # Publish the file content to the MQTT broker
            except Exception as e:
                print(f"Error processing data: {e}", json_str)

    # Extract MAC address from filename (assuming MAC is before the first underscore)

    print(f"Publishing to MQTT topic: {mqtt_topic}")

    # Publish the file content to the MQTT broker

    # Cleanup the local file (if needed)
    os.remove(local_filename)


# Main loop
def main():
    mqtt_client = connect_to_mqtt()  # Connect to MQTT broker
    while True:
        try:
            ftp = connect_to_ftp()
            check_and_process_files(ftp, mqtt_client)
            ftp.quit()
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(1)  # Check every second


if __name__ == "__main__":
    main()
