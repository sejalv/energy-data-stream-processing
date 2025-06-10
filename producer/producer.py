# producer.py

import os
import json
import time
import random
import logging
import argparse

from kafka import KafkaProducer
from prometheus_client import Counter, start_http_server

# Prometheus metrics
EVENTS_SENT = Counter('producer_events_sent_total', 'Total events sent by producer', ['status'])
KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")

REQUIRED_KEYS = {"event_type", "event_time", "payload"}
TOPIC = "energy_events"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# EVENTS_SENT.labels(status='success')
# EVENTS_SENT.labels(status='failure')

def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, help="Path to input events file")
    parser.add_argument("--error_file", type=str, help="Path to bad events log")
    parser.add_argument("--shuffle", action="store_true", help="Shuffle events before sending")
    parser.add_argument("--delay", type=float, default=1.0, help="Avg delay between events (in seconds)")
    parser.add_argument("--topic", type=str, default=TOPIC, help="Kafka topic to send events to")
    return parser

def log_bad_event(event_line, reason, log_path):
    """Log events that failed validation to error file"""
    EVENTS_SENT.labels(status='failure').inc()
    with open(log_path, "a") as f:
        f.write(f"# {reason}\n{event_line.strip()}\n")

def load_events(file_path, log_path, shuffle=False):
    """Load events from JSONL file, handling JSON parsing errors"""
    with open(file_path) as f:
        lines = f.readlines()
    if shuffle:
        random.shuffle(lines)
    for line in lines:
        try:
            event = json.loads(line)
            yield event, line
        except json.JSONDecodeError:
            logger.error("Malformed JSON skipped: %s", line.strip())
            log_bad_event(line, "Malformed JSON", log_path)

def validate_event(event):
    """
        Generic syntax validation - only checks basic event structure.
        Domain-specific validation should be handled by consumers.
    """
    if not isinstance(event, dict):
        return False, "Event is not a dictionary"
    missing = REQUIRED_KEYS - event.keys()
    if missing:
        return False, f"Missing required keys: {missing}"
    if not event.get("event_type").strip() or not event.get("payload"):
        return False, "event_type or payload is empty"
    return True, ""

def send_event(producer, topic, event):
    try:
        producer.send(topic, event).get(timeout=10)  # wait for ack/failure
        EVENTS_SENT.labels(status='success').inc()
    except Exception as e:
        logger.error("Send failed: %s", e)
        EVENTS_SENT.labels(status='failure').inc()

def main():
    parser = create_parser()
    args = parser.parse_args()

    start_http_server(9090, addr="0.0.0.0")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all',  # Wait for all replicas to acknowledge
        retries=5,  # Retry failed sends
        max_in_flight_requests_per_connection=1,  # Ensure ordering
        buffer_memory=33554432, # 32 MB
        linger_ms=0,
        batch_size=1
    )
    logging.info(f"Connected to Kafka at {KAFKA_BROKER}")
    logging.info(f"Sending events to topic '{args.topic}'...")

    for event, raw_line in load_events(args.file, args.error_file, shuffle=args.shuffle):
        valid, reason = validate_event(event)
        if not valid:
            log_bad_event(raw_line, reason, args.error_file)
            logger.warning("Invalid event: %s", reason)
            continue

        send_event(producer, args.topic, event)
        logger.info(f"Sent: {event}")
        time.sleep(random.uniform(args.delay * 0.5, args.delay * 1.5))  # producer as a simulator sends events at random times

    producer.flush()
    logger.info("Finished sending events.")
    logger.info("Waiting for final metrics to be scraped...")
    time.sleep(10)

if __name__ == "__main__":
    main()
