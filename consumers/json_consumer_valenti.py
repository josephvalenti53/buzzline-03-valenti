import os
import json
from collections import defaultdict
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Load Environment Variables
load_dotenv()

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> int:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

# Set up Data Store to hold author counts
author_counts = defaultdict(int)

# Simpsons-related catchphrase analysis
catchphrases = {
    "Homer": ["D'oh!", "Mmm", "Woo-hoo!"],
    "Marge": ["Put a little elbow grease in it", "Homer, stop it!", "I'm not a nag!"],
    "Lisa": ["If anyone can do it, I can", "I can't do it, I need to practice", "I'm the voice of a generation"],
    "Moe": ["Hey, hey, hey", "I'm a little off today", "I got a headache"],
    "Barney": ["No problem", "Yeehaw!", "I don’t drink that much"],
    "Otto": ["Relax, man!", "The world’s a better place with a little more rock n roll!", "Time to rock!" ]
}

# Custom Logging Function for Simpsons Characters
def log_custom_message(author: str, count: int):
    if author == "Homer" and count % 10 == 0:
        logger.info(f"Homer's done it again! {count} messages... D'oh!")
    elif author == "Marge" and count % 10 == 0:
        logger.info(f"Marge is on fire! {count} messages... Put a little elbow grease in it!")
    elif author == "Lisa" and count % 10 == 0:
        logger.info(f"Lisa’s intellect knows no bounds! {count} messages... I'm the voice of a generation!")
    elif author == "Moe" and count % 10 == 0:
        logger.info(f"Moe’s tavern is getting popular! {count} messages... 'Hey, hey, hey!'")
    elif author == "Barney" and count % 10 == 0:
        logger.info(f"Barney’s taking over! {count} messages... No problem!")
    elif author == "Otto" and count % 10 == 0:
        logger.info(f"Otto is rocking it! {count} messages... Time to rock!")
    else:
        logger.info(f"{author} just said their catchphrase: {catchphrases.get(author, ['No phrase'])[0]}")

# Function to process a single message
def process_message(message: str) -> None:
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)

        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Ensure it's a dictionary before accessing fields
        if isinstance(message_dict, dict):
            # Extract the 'author' field from the Python dictionary
            author = message_dict.get("author", "unknown")
            logger.info(f"Message received from author: {author}")

            # Increment the count for the author
            author_counts[author] += 1

            # Custom logging after certain milestones
            log_custom_message(author, author_counts[author])

            # Log the updated counts
            logger.info(f"Updated author counts: {dict(author_counts)}")

        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

# Main function
def main() -> None:
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

# Conditional Execution
if __name__ == "__main__":
    main()
