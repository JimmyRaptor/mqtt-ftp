from datetime import datetime
import logging
from typing import Optional


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


def normalize_timestamp(timestamp: float) -> Optional[str]:
    try:
        # Convert timestamp to datetime
        date = datetime.utcfromtimestamp(timestamp)
        current_year = datetime.now().year

        # Check if the timestamp is from this year
        if date.year == current_year:
            # Format the date and return
            formatted_date = f"{date:%Y-%m-%d %H:%M:%S}+00"
            return formatted_date
        else:
            # Add 30 years to the timestamp
            new_year = date.year + 30

            # Check if the new year is this year
            if new_year == current_year:
                date = date.replace(year=new_year)
                formatted_date = f"{date:%Y-%m-%d %H:%M:%S}+00"
                return formatted_date
            else:
                # If neither the original nor the adjusted year is this year, raise an error
                raise ValueError("Timestamp is not within the expected range.")

    except ValueError as e:
        # Handle the error case
        logging.error(f"Error normalizing timestamp: {e}")
        date = datetime.utcfromtimestamp(0)
        formatted_date = f"{date:%Y-%m-%d %H:%M:%S}+00"
        return formatted_date

    except Exception:
        return None
