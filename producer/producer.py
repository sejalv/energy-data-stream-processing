# producer.py

import json
import time
import os
import logging
from kafka import KafkaProducer
import argparse
import random

from prometheus_client import Counter, start_http_server

# Prometheus metrics
EVENTS_SENT = Counter('producer_events_sent_total', 'Total events sent by producer', ['status'])

TOPIC = "user_events"
KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
REQUIRED_KEYS = {"event_type", "event_time", "payload"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

EVENTS_SENT.labels(status='success')
EVENTS_SENT.labels(status='failure')

def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, default="data/events.jsonl", help="Path to input events file")
    parser.add_argument("--error_file", type=str, default="data/bad_events.log", help="Path to bad events log")
    parser.add_argument("--shuffle", action="store_true", help="Shuffle events before sending")
    parser.add_argument("--delay", type=float, default=1.0, help="Avg delay between events (in seconds)")
    return parser

def log_bad_event(event_line, reason, log_path):
    EVENTS_SENT.labels(status='failure').inc()
    with open(log_path, "a") as f:
        f.write(f"# {reason}\n{event_line.strip()}\n")

# Can be replaced by another type of connector
def load_events(file_path, log_path, shuffle=False):
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
    if not isinstance(event, dict):
        return False, "Not a dictionary"
    missing = REQUIRED_KEYS - event.keys()
    if missing:
        return False, f"Missing keys: {missing}"
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
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    logging.info(f"Connected to Kafka at {KAFKA_BROKER}")
    logging.info(f"Sending events to topic '{TOPIC}'...")

    for event, raw_line in load_events(args.file, args.error_file, shuffle=args.shuffle):
        valid, reason = validate_event(event)
        if not valid:
            log_bad_event(raw_line, reason, args.error_file)
            logger.warning("Invalid event: %s", reason)
            continue

        send_event(producer, TOPIC, event)
        logger.info(f"Sent: {event}")
        time.sleep(random.uniform(args.delay * 0.5, args.delay * 1.5))

    producer.flush()
    logger.info("Finished sending events.")

if __name__ == "__main__":
    main()
