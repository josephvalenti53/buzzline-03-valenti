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
# Message Generator
#####################################

# Historical Facts Lists
historical_bbq_facts = [
    "The word "barbecue" comes from the Caribbean – The term originates from the Taíno word barbacoa, which referred to a wooden structure used for cooking meat over an open flame.",
    "George Washington loved BBQ – The first U.S. president was known to host barbecues at his Mount Vernon estate. His diary even mentions attending a "barbicue" in 1769.",
    "Texas BBQ dates back to the 1800s – German and Czech immigrants brought their smoking techniques to Texas, blending them with local beef culture to create what we now call Texas BBQ.",
    "Kansas City BBQ was influenced by a street vendor – Henry Perry, known as the "father of Kansas City BBQ," started selling slow-smoked meats from a cart in the early 1900s, helping define the city's iconic BBQ style.",
    "Memphis BBQ got famous through river trade – In the 19th century, Memphis became a BBQ hub thanks to its location along the Mississippi River, where pork was plentiful and slow-cooked with a dry rub.",
    "The oldest BBQ joint in the U.S. is still running – Southside Market & Barbecue in Elgin, Texas, was established in 1882 and is still serving up legendary smoked meats.",
    "North Carolina BBQ has a Civil War connection – The vinegar-based BBQ sauce commonly found in North Carolina dates back to the Civil War era when preservation was key to keeping meat edible for longer periods.",
    "Alabama's famous white sauce was created in 1925 – Big Bob Gibson in Decatur, Alabama, invented the tangy, mayo-based white sauce that became a staple for smoked chicken.",
    "BBQ competitions go way back – The first recorded BBQ competition in the U.S. took place in the early 20th century, but BBQ cook-offs gained national popularity in the 1980s with the rise of the Kansas City Barbeque Society (KCBS).",
    "Barbecue was used as a political tool – In the 1800s, American politicians hosted "BBQ rallies," feeding people smoked meat to gain votes and build political support.",
    "The first recorded BBQ in America was in 1540.",
    "Native Americans influenced early BBQ techniques.",
    "Barbecue was a staple in colonial America.",
    "BBQ pitmasters were once called 'fire cooks'.",
    "BBQ played a role in Texas cattle culture.",
    "BBQ was an early American community event.",
    "The first BBQ restaurant in the U.S. opened in 1919.",
    "South Carolina has the most diverse BBQ sauces.",
    "Slavery played a major role in BBQ history.",
    "BBQ was used as a form of payment in the 1800s.",
    "The term 'pit barbecue' refers to its original cooking method.",
    "Barbecue was part of early U.S. presidential campaigns.",
    "Hawaiian BBQ has ancient roots in the imu cooking method.",
    "Chicago had a thriving BBQ scene in the early 1900s.",
    "The famous BBQ 'bark' comes from a chemical reaction.",
    "BBQ became commercialized in the 20th century.",
    "Barbecue diplomacy is a real thing in politics.",
    "The world's largest BBQ pit is in Texas.",
    "Korean BBQ dates back to ancient times.",
    "Argentina’s BBQ culture (Asado) is legendary.",
    "NASA sent BBQ-flavored food to space.",
    "The world’s largest BBQ festival is in Memphis.",
    "The oldest BBQ cookbook was published in 1748.",
    "BBQ competitions exploded in the 1980s.",
    "Pulled pork has Caribbean origins.",
    "The first recorded BBQ in America was in 1540 – Spanish explorer Hernando de Soto and his men reportedly encountered indigenous tribes slow-cooking meat over wooden frames in what is now the southeastern U.S.",
    "Native Americans influenced early BBQ techniques – Indigenous tribes like the Cherokee and Choctaw were already smoking and slow-cooking meat long before European settlers arrived.",
    "Barbecue was a staple in colonial America – By the 1700s, BBQ was a common way to prepare meat, especially for large gatherings and celebrations.",
    "BBQ pitmasters were once called 'fire cooks' – In the early days, BBQ chefs were known as 'fire cooks' because of their expertise in managing the flames and smoke.",
    "BBQ played a role in Texas cattle culture – In the 1800s, Texas cowboys and ranchers slow-cooked tough cuts of beef to make them tender, helping create the brisket-centered BBQ style we know today.",
    "BBQ was an early American community event – In the 19th century, BBQs were often held as town-wide events, where entire communities gathered for slow-cooked feasts.",
    "The first BBQ restaurant in the U.S. opened in 1919 – Payne’s Bar-B-Q in Memphis, Tennessee, is believed to be one of the first official BBQ restaurants, still famous for its smoked meats and mustard-based slaw.",
    "South Carolina has the most diverse BBQ sauces – South Carolina is unique for having four main styles of BBQ sauce: mustard-based, vinegar-pepper, light tomato, and heavy tomato.",
    "Slavery played a major role in BBQ history – Many of the pitmasters who perfected BBQ in the South were enslaved people who passed down their techniques through generations.",
    "BBQ was used as a form of payment – In the 1800s, some workers were paid in BBQ, especially in rural areas where cash was scarce.",
    "The term 'pit barbecue' refers to its original cooking method – Traditionally, BBQ was cooked in a pit dug into the ground, a technique still used in some places today.",
    "Barbecue was part of early U.S. presidential campaigns – Presidents like Andrew Jackson and James K. Polk hosted massive BBQ rallies to attract voters.",
    "Hawaiian BBQ has ancient roots – The Hawaiian imu, an underground oven, has been used for centuries to slow-cook meats like pork (kalua pig), similar to Southern BBQ techniques.",
    "Chicago had a thriving BBQ scene in the early 1900s – The Great Migration brought BBQ traditions from the South to Chicago, where it evolved into a distinct style.",
    "The famous BBQ 'bark' comes from a chemical reaction – The flavorful, crispy crust on smoked meat is formed through the Maillard reaction, a chemical process that occurs when proteins and sugars interact with heat.",
    "BBQ became commercialized in the 20th century – With the rise of roadside BBQ stands and drive-ins in the 1920s and 1930s, BBQ became an American fast-food favorite.",
    "Barbecue diplomacy is a real thing – U.S. presidents have used BBQ as a diplomatic tool, serving world leaders smoked meats to build relationships.",
    "The world’s largest BBQ pit is in Texas – The 'Undisputable Cuz' is a massive 76-foot-long BBQ pit, capable of smoking over 4,000 pounds of meat at once.",
    "Korean BBQ dates back to ancient times – Gogigui, or Korean BBQ, has been around for centuries, with early versions involving meat grilled on stones.",
    "Argentina’s BBQ culture (Asado) is legendary – Dating back to the 16th century, asado (Argentine BBQ) is a national tradition where large cuts of beef are slow-cooked over wood fires.",
    "NASA sent BBQ to space – Astronauts on the Apollo missions were given BBQ-flavored space food to remind them of home.",
    "The world’s largest BBQ festival is in Memphis – The 'Memphis in May World Championship Barbecue Cooking Contest' attracts teams from all over the world, competing for BBQ glory.",
    "The oldest BBQ cookbook was published in 1748 – The Art of Cookery Made Plain and Easy by Hannah Glasse included one of the earliest written BBQ recipes.",
    "BBQ competitions exploded in the 1980s – The rise of the Kansas City Barbeque Society (KCBS) helped turn BBQ into a competitive sport.",
    "Pulled pork has Caribbean origins – The slow-cooked, shredded pork dish popular in the South traces its roots back to Caribbean and indigenous Taino cooking methods."
]

def generate_messages(file_path: pathlib.Path):
    """
    Read from a CSV file and yield records one by one, continuously.

    Args:
        file_path (pathlib.Path): Path to the CSV file.

    Yields:
        dict: Message containing timestamp, temperature, and historical fact.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {DATA_FILE}")
            with open(DATA_FILE, "r") as csv_file:
                logger.info(f"Reading data from file: {DATA_FILE}")

                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    # Ensure required fields are present
                    if "temperature" not in row:
                        logger.error(f"Missing 'temperature' column in row: {row}")
                        continue

                    # Generate a timestamp and prepare the message
                    current_timestamp = datetime.utcnow().isoformat()
                    historical_fact = random.choice(historical_bbq_facts)  # Select a random fact

                    message = {
                        "timestamp": current_timestamp,
                        "temperature": float(row["temperature"]),
                        "historical_fact": historical_fact,  # Add fact to message
                    }
                    logger.debug(f"Generated message: {message}")
                    yield message
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
