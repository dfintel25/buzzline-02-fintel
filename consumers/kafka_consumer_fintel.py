"""
Kafka Consumer Script
File: consumers/kafka_consumer_fintel.py

Consume messages from a Kafka topic and log them.
"""

#####################################
# Import Modules
#####################################

# Standard Library
import os
import sys
import json

# External
from dotenv import load_dotenv

# Local
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger


#####################################
# Getter Functions
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "buzz_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group ID from environment or use default."""
    group_id = os.getenv("KAFKA_CONSUMER_GROUP", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Message Handler
#####################################


def handle_message(message):
    """Decode and log Kafka messages."""
    try:
        payload = json.loads(message.value.decode("utf-8"))
        logger.info(f"Consumed buzz: {payload}")
    except Exception as e:
        logger.error(f"Failed to decode Kafka message: {e}")


#####################################
# Main Function
#####################################


def main():
    """Main entry point for this consumer."""
    logger.info("START consumer.")
    load_dotenv()

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    consumer = None
    try:
        consumer = create_kafka_consumer(topic, group_id)
        logger.info(f"Polling messages from topic '{topic}'...")

        for message in consumer:
            handle_message(message)

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        if consumer:
            consumer.close()
            logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
