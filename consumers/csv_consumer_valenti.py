"""
csv_consumer_joseph.py

Consume json messages from a Kafka topic and process them.

Example Kafka message format:
{"timestamp": "2025-01-11T18:15:00Z", "temperature": 225.0}
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json

# Use a deque ("deck") - a double-ended queue data structure
from collections import deque

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

# Load environment variables from .env
load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_stall_threshold() -> float:
    """Fetch message interval from environment or use default."""
    temp_variation = float(os.getenv("SMOKER_STALL_THRESHOLD_F", 0.2))
    logger.info(f"Max stall temperature range: {temp_variation} F")
    return temp_variation

def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size

def get_temperature_change_threshold() -> float:
    """Fetch threshold for significant temperature change."""
    temp_change_threshold = float(os.getenv("TEMP_CHANGE_THRESHOLD_F", 5.0))
    logger.info(f"Temperature change threshold: {temp_change_threshold}°F")
    return temp_change_threshold

#####################################
# Define functions for stall detection and alerting on significant temperature changes
#####################################

def detect_stall(rolling_window_deque: deque) -> bool:
    """Detect a temperature stall based on the rolling window."""
    WINDOW_SIZE: int = get_rolling_window_size()
    if len(rolling_window_deque) < WINDOW_SIZE:
        logger.debug(f"Rolling window size: {len(rolling_window_deque)}. Waiting for {WINDOW_SIZE}.")
        return False

    temp_range = max(rolling_window_deque) - min(rolling_window_deque)
    is_stalled: bool = temp_range <= get_stall_threshold()
    logger.debug(f"Temperature range: {temp_range}°F. Stalled: {is_stalled}")
    return is_stalled

def check_for_temperature_change(temperature: float, previous_temperature: float) -> None: #checks for temp change
    """Check if the temperature change exceeds the threshold."""
    temp_change_threshold = get_temperature_change_threshold()
    change = abs(temperature - previous_temperature)
    if change > temp_change_threshold:
        logger.warning(f"Significant temperature change detected: {change}°F from {previous_temperature}°F to {temperature}°F.")
        # You can also add logic here for more actions (e.g., trigger alerts, notifications)
    return temperature

#####################################
# Function to process a single message
#####################################

def process_message(message: str, rolling_window: deque, window_size: int, previous_temperature: float) -> None:
    """Process a JSON message and check for stalls or significant temperature changes."""
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        logger.info(f"Processed JSON message: {data}")

        # Ensure the required fields are present
        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Check for significant temperature change
        previous_temperature = check_for_temperature_change(temperature, previous_temperature)

        # Append the temperature reading to the rolling window
        rolling_window.append(temperature)

        # Check for a stall
        if detect_stall(rolling_window):
            logger.info(f"STALL DETECTED at {timestamp}: Temp stable at {temperature}°F over last {window_size} readings.")

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

#####################################
# Define main function for this module
#####################################

def main() -> None:
    """Main entry point for the consumer."""
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)
    previous_temperature = None  # Initialize previous_temperature

    # Create the Kafka consumer
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size, previous_temperature)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
