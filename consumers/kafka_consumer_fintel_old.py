"""
kafka_consumer_fintel.py

Consume messages from a Kafka topic and process them.
"""

#####################################
# Import Modules
#####################################
import json
from datetime import datetime, timezone

# Import packages from Python Standard Library
import os

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
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
    topic = os.getenv("KAFKA_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> int:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Define a function to process a single message
# #####################################


'''def process_message(message: str) -> None:
    """
    Process a single message.

    For now, this function simply logs the message.
    You can extend it to perform other tasks, like counting words
    or storing data in a database.

    Args:
        message (str): The message to process.
    """
    logger.info(f"Processing message: {message}")
'''
def process_message(message: bytes) -> None:
    """
    Process a single Kafka message.
    Expects JSON dict with 'timestamp' and 'message'.
    Adds a replay flag if the timestamp is older than now by >5 minutes.
    """
    try:
        # Decode JSON
        payload = json.loads(message.decode("utf-8"))
        
        text = payload.get("message", "<no text>")
        ts_str = payload.get("timestamp", None)
        replay_flag = ""

        # Replay awareness: flag if timestamp is older than 5 minutes
        if ts_str:
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - ts).total_seconds()
                if age > 300:  # older than 5 minutes
                    replay_flag = "[REPLAY]"
            except Exception:
                logger.warning(f"Could not parse timestamp '{ts_str}'")

        logger.info(f"{replay_flag} Processing message: {text} (ts={ts_str})")

    except Exception as e:
        logger.error(f"Failed to process message {message}: {e}")


def handle_message(message):
    try:
        payload = json.loads(message.value.decode("utf-8"))
        logger.info(f"Consumed buzz: {payload}")
    except Exception as e:
        logger.error(f"Failed to decode Kafka message: {e}")
#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

     # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message.value)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
