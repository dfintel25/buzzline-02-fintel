"""
Kafka Producer Script
File: producers/kafka_producer_fintel.py

Produce some streaming buzz strings and send them to a Kafka topic.
"""

#####################################
# Import Modules
#####################################

# Standard Library
import os
import sys
import time
import json
from datetime import datetime, timezone
import random

# External
from dotenv import load_dotenv

# Local
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger


#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "buzz_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Message Generator
#####################################

MESSAGES: list[str] = [
    "I love Python!",
    "Kafka is awesome.",
    "Streaming data is fun.",
    "This is a buzz message.",
    "Have a great day!",
]


def generate_messages(producer, topic, interval_secs: int):
    """
    Generate a stream of buzz messages and send them to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send messages to.
        interval_secs (int): Time in seconds between sending messages.
    """
    try:
        while True:
            message_text = random.choice(MESSAGES)
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message": message_text,
            }

            # Log clean dict
            logger.info(f"Generated buzz: {payload}")

            # ✅ Correct: send dict, Kafka serializer handles encoding
            producer.send(topic, value=payload)
            logger.info(f"Sent message to topic '{topic}': {payload}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in message generation: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


#####################################
# Main Function
#####################################


def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer.
    - Streams generated buzz message strings to the Kafka topic.
    """
    logger.info("START producer.")
    logger.info("Loading environment variables from .env file...")
    load_dotenv()
    verify_services()

    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Create the Kafka producer
    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Ensure topic exists
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    generate_messages(producer, topic, interval_secs)

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
