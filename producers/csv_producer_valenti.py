"""
csv_producer_valenti.py

Stream numeric data to a Kafka topic.

It is common to transfer csv data as JSON so 
each field is clearly labeled. 
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time  # control message intervals
import pathlib  # work with file paths
import csv  # handle CSV data
import json  # work with JSON data
import random # gotta make it interesting
from datetime import datetime  # work with timestamps

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("SMOKER_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval

#####################################
# Set up Paths
#####################################

# The parent directory of this file is its folder.
# Go up one more parent level to get the project root.
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where data is stored
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

# Set the name of the data file
DATA_FILE = DATA_FOLDER.joinpath("smoker_temps.csv")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Historical Facts Lists
#####################################

historical_bbq_facts = [
    "The word 'barbecue' comes from the Caribbean – The term originates from the Taíno word barbacoa, which referred to a wooden structure used for cooking meat over an open flame.",
    "George Washington loved BBQ – The first U.S. president was known to host barbecues at his Mount Vernon estate. His diary even mentions attending a 'barbicue' in 1769.",
    "Texas BBQ dates back to the 1800s – German and Czech immigrants brought their smoking techniques to Texas, blending them with local beef culture to create what we now call Texas BBQ.",
    "Kansas City BBQ was influenced by a street vendor – Henry Perry, known as the 'father of Kansas City BBQ,' started selling slow-smoked meats from a cart in the early 1900s, helping define the city's iconic BBQ style.",
    "Memphis BBQ got famous through river trade – In the 19th century, Memphis became a BBQ hub thanks to its location along the Mississippi River, where pork was plentiful and slow-cooked with a dry rub.",
    "The oldest BBQ joint in the U.S. is still running – Southside Market & Barbecue in Elgin, Texas, was established in 1882 and is still serving up legendary smoked meats.",
    "North Carolina BBQ has a Civil War connection – The vinegar-based BBQ sauce commonly found in North Carolina dates back to the Civil War era when preservation was key to keeping meat edible for longer periods.",
    "Alabama's famous white sauce was created in 1925 – Big Bob Gibson in Decatur, Alabama, invented the tangy, mayo-based white sauce that became a staple for smoked chicken.",
    "BBQ competitions go way back – The first recorded BBQ competition in the U.S. took place in the early 20th century, but BBQ cook-offs gained national popularity in the 1980s with the rise of the Kansas City Barbeque Society (KCBS).",
    "Barbecue was used as a political tool – In the 1800s, American politicians hosted 'BBQ rallies,' feeding people smoked meat to gain votes and build political support.",
    # Add more facts as necessary
]

#####################################
# Message Generator
#####################################

def generate_messages(file_path: pathlib.Path):
    """
    Read from a CSV file and yield records one by one, continuously.
    Every 10th message will include a historical BBQ fact.

    Args:
        file_path (pathlib.Path): Path to the CSV file.

    Yields:
        dict: Message containing timestamp, temperature, and (optionally) historical fact.
    """
    count = 0  # Counter to track every 10th message

    while True:
        try:
            logger.info(f"Opening data file in read mode: {DATA_FILE}")
            with open(file_path, "r") as csv_file:
                logger.info(f"Reading data from file: {file_path}")

                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    # Ensure required fields are present
                    if "temperature" not in row:
                        logger.error(f"Missing 'temperature' column in row: {row}")
                        continue

                    # Generate a timestamp and prepare the message
                    current_timestamp = datetime.utcnow().isoformat()

                    # For every 10th message, include a historical BBQ fact
                    if count % 10 == 0:
                        historical_fact = random.choice(historical_bbq_facts)
                        message = {
                            "timestamp": current_timestamp,
                            "temperature": float(row["temperature"]),
                            "historical_fact": historical_fact  # Include fact
                        }
                    else:
                        message = {
                            "timestamp": current_timestamp,
                            "temperature": float(row["temperature"]),
                        }
                    
                    logger.debug(f"Generated message: {message}")
                    yield message

                    count += 1  # Increment the counter
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)

#####################################
# Define main function for this module.
#####################################

def main():
    """
    Main entry point for the producer.

    - Reads the Kafka topic name from an environment variable.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams messages to the Kafka topic.
    """

    logger.info("START producer.")
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Verify the data file exists
    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for csv_message in generate_messages(DATA_FILE):
            producer.send(topic, value=csv_message)
            logger.info(f"Sent message to topic '{topic}': {csv_message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
