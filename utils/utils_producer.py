# utils_producer.py - common functions used by producers.

#####################################
# Import Modules
#####################################

import os
import sys
import time
import json

from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer, errors
from kafka.admin import (
    KafkaAdminClient,
    ConfigResource,
    ConfigResourceType,
    NewTopic,
)

from utils.utils_logger import logger


#####################################
# Default Configurations
#####################################

DEFAULT_KAFKA_BROKER_ADDRESS = "localhost:9092"


#####################################
# Helper Functions
#####################################

def get_kafka_broker_address():
    broker_address = os.getenv("KAFKA_BROKER_ADDRESS", DEFAULT_KAFKA_BROKER_ADDRESS)
    logger.info(f"Kafka broker address: {broker_address}")
    return broker_address


#####################################
# Kafka Readiness Check
#####################################

def check_kafka_service_is_ready():
    kafka_broker = get_kafka_broker_address()
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
        brokers = admin_client.describe_cluster()
        logger.info(f"Kafka is ready. Brokers: {brokers}")
        admin_client.close()
        return True
    except errors.KafkaError as e:
        logger.error(f"Error checking Kafka: {e}")
        return False


#####################################
# Kafka Producer and Topic Management
#####################################

def verify_services():
    if not check_kafka_service_is_ready():
        logger.error("Kafka broker is not ready. Exiting...")
        sys.exit(2)


def create_kafka_producer(value_serializer=None):
    """
    Create and return a Kafka producer instance.
    Default serializer: JSON → UTF-8 bytes
    """
    kafka_broker = get_kafka_broker_address()

    if value_serializer is None:
        def value_serializer(x):
            return json.dumps(x).encode("utf-8")

    try:
        logger.info(f"Connecting to Kafka broker at {kafka_broker}...")
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=value_serializer,
        )
        logger.info("Kafka producer successfully created.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None


def create_kafka_topic(topic_name, group_id=None):
    kafka_broker = get_kafka_broker_address()
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
        topics = admin_client.list_topics()
        if topic_name in topics:
            logger.info(f"Topic '{topic_name}' already exists. Clearing it out...")
            clear_kafka_topic(topic_name, group_id)
        else:
            logger.info(f"Creating '{topic_name}'.")
            new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics([new_topic])
            logger.info(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        logger.error(f"Error managing topic '{topic_name}': {e}")
        sys.exit(1)
    finally:
        admin_client.close()


def clear_kafka_topic(topic_name, group_id):
    kafka_broker = get_kafka_broker_address()
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
    try:
        config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
        configs = admin_client.describe_configs([config_resource])
        original_retention = configs[config_resource].get("retention.ms", "604800000")
        logger.info(f"Original retention.ms for '{topic_name}': {original_retention}")

        admin_client.alter_configs({config_resource: {"retention.ms": "1"}})
        logger.info(f"Retention.ms temporarily set to 1ms for '{topic_name}'.")

        time.sleep(2)

        logger.info(f"Clearing topic '{topic_name}' by consuming all messages...")
        consumer = KafkaConsumer(
            topic_name,
            group_id=group_id,
            bootstrap_servers=kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        for message in consumer:
            logger.debug(f"Clearing message: {message.value}")
        consumer.close()
        logger.info(f"All messages cleared from topic '{topic_name}'.")

        admin_client.alter_configs({config_resource: {"retention.ms": original_retention}})
        logger.info(f"Retention.ms restored for '{topic_name}'.")
    except Exception as e:
        logger.error(f"Error managing retention for topic '{topic_name}': {e}")
    finally:
        admin_client.close()


#####################################
# Main Function for Testing
#####################################

def main():
    logger.info("Starting utils_producer.py script...")
    logger.info("Loading environment variables from .env file...")
    load_dotenv()

    if not check_kafka_service_is_ready():
        logger.error("Kafka is not ready. Exiting.")
        sys.exit(2)

    logger.info("All services are ready. Proceeding with producer setup.")
    create_kafka_topic("test_topic", "default_group")


if __name__ == "__main__":
    main()
