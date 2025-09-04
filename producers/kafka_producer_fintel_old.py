# kafka_producer_fintel.py
# Produce some streaming buzz strings and send them to a Kafka topic.

import os
import sys
import time
from datetime import datetime, timezone, time as dt_time
from dotenv import load_dotenv
import json

from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger


def get_kafka_topic() -> str:
    topic = os.getenv("KAFKA_TOPIC", "buzz_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


def in_recording_window() -> bool:
    now = datetime.now(timezone.utc).time()
    return dt_time(0, 0) <= now <= dt_time(6, 0)


def generate_messages(producer, topic, interval_secs, record_file="sent_messages.log"):
    string_list = [
        "I love Python!",
        "Kafka is awesome.",
        "Streaming data is fun.",
        "This is a buzz message.",
        "Have a great day!",
    ]
    try:
        with open(record_file, "a", encoding="utf-8") as f:
            while True:
                for message in string_list:
                    payload = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "message": message,
                    }
                    logger.info(f"Generated buzz: {payload}")
                    producer.send(topic, value=json.dumps(payload).encode("utf-8"))
                    logger.info(f"Sent message to topic '{topic}': {payload}")

                    if in_recording_window():
                        f.write(f"{payload}\n")
                        f.flush()

                    # Use standard time module sleep
                    time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in message generation: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


def replay_messages(producer, topic, record_file="sent_messages.log", interval_secs=1):
    try:
        with open(record_file, "r", encoding="utf-8") as f:
            for line in f:
                payload = eval(line.strip())  # replay dict
                logger.info(f"Replaying message: {payload}")
                producer.send(topic, value=json.dumps(payload).encode("utf-8"))
                time.sleep(interval_secs)
    except FileNotFoundError:
        logger.error(f"No replay log found at {record_file}")
    except Exception as e:
        logger.error(f"Error during replay: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed after replay.")


def main():
    logger.info("START producer.")
    logger.info("Loading environment variables from .env file...")
    load_dotenv()
    verify_services()

    mode = os.getenv("MODE", "produce")
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    if mode == "replay":
        logger.info(f"Replaying messages from log to topic '{topic}'...")
        replay_messages(producer, topic, interval_secs=interval_secs)
    else:
        logger.info(f"Starting message production to topic '{topic}'...")
        generate_messages(producer, topic, interval_secs)

    logger.info("END producer.")


if __name__ == "__main__":
    main()
