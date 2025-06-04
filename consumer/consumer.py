# consumer.py
import os
import time
import json
import logging
import psycopg2
import threading
from datetime import datetime
from typing import Dict, Set
from dataclasses import dataclass, field
from collections import defaultdict
from kafka import KafkaConsumer
from psycopg2 import sql
from prometheus_client import Counter, Histogram, start_http_server

# Config
TOPIC = "user_events"
GROUP_ID = "aggregator_group"
KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
PG_CONN = os.getenv("POSTGRES_CONN", f"dbname={os.getenv('POSTGRES_DB')} user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')} host={os.getenv('POSTGRES_HOST')} port=5432")

# Prometheus Metrics
EVENTS_RECEIVED = Counter('events_received_total', 'Events received from Kafka', ['status'])
PROCESSING_TIME = Histogram('event_processing_seconds', 'Time taken to process each event')

EVENTS_RECEIVED.labels(status='success')
EVENTS_RECEIVED.labels(status='failure')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

@dataclass
class HourlyStats:
    total_revenue: float = 0.0
    ticket_count: int = 0
    total_payouts: float = 0.0
    payout_events: int = 0
    active_users: Set[str] = field(default_factory=set)
    new_sessions: Set[str] = field(default_factory=set)
    total_logins: int = 0

business_stats: Dict[datetime, HourlyStats] = defaultdict(HourlyStats)

def validate_event(event: Dict) -> bool:
    required_fields = {"user_id", "session_id"}
    if not required_fields.issubset(event["payload"].keys()):
        logging.warning("Missing fields in payload: %s", required_fields - event["payload"].keys())
        return False
    return True

def ingest_event_raw(cursor, event: Dict) -> bool:
    try:
        cursor.execute(
            sql.SQL("""
                INSERT INTO events (user_id, event_type, event_time, session_id, amount)
                VALUES (%s, %s, %s, %s, %s)
            """),
            (
                event["payload"]["user_id"],
                event["event_type"],
                event["event_time"],
                event["payload"]["session_id"],
                event["payload"].get("amount")
            )
        )
        return True
    except Exception:
        logging.exception("Failed to insert event into raw table")
        return False

# Business Logic
@PROCESSING_TIME.time()
def process_event(event: Dict, business_stats: Dict[datetime, HourlyStats]):
    """Aggregate event into hourly business metrics"""
    payload = event["payload"]
    event_type = event["event_type"]
    user_id = payload["user_id"]
    session_id = payload["session_id"]
    dt = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
    hour = dt.replace(minute=0, second=0, microsecond=0)

    stats = business_stats[hour]

    if event_type == "ticket_purchased":
        amount = float(payload.get("amount", 0))
        stats.total_revenue += amount
        stats.ticket_count += 1

    elif event_type == "claim_reward":
        amount = float(payload.get("amount", 0))
        stats.total_payouts += amount
        stats.payout_events += 1

    elif event_type == "login":
        stats.total_logins += 1
        stats.new_sessions.add(session_id)

    stats.active_users.add(user_id)


def flush_business_metrics(cursor, business_stats: Dict[datetime, HourlyStats]):
    """Persist aggregated hourly metrics into DB"""
    try:
        for hour, stats in business_stats.items():
            avg_ticket_value = stats.total_revenue / stats.ticket_count if stats.ticket_count > 0 else 0
            payout_ratio = stats.total_payouts / stats.total_revenue if stats.total_revenue > 0 else None
            active_user_count = len(stats.active_users)
            new_session_count = len(stats.new_sessions)

            cursor.execute("""
                INSERT INTO hourly_business_metrics (
                    hour, total_revenue, ticket_count, avg_ticket_value,
                    total_payouts, payout_events, payout_to_revenue_ratio,
                    active_users, new_sessions, total_logins
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (hour) DO UPDATE SET
                    total_revenue = hourly_business_metrics.total_revenue + EXCLUDED.total_revenue,
                    ticket_count = hourly_business_metrics.ticket_count + EXCLUDED.ticket_count,
                    avg_ticket_value = CASE
                        WHEN (hourly_business_metrics.ticket_count + EXCLUDED.ticket_count) > 0
                        THEN (hourly_business_metrics.total_revenue + EXCLUDED.total_revenue) /
                             (hourly_business_metrics.ticket_count + EXCLUDED.ticket_count)
                        ELSE 0
                    END,
                    total_payouts = hourly_business_metrics.total_payouts + EXCLUDED.total_payouts,
                    payout_events = hourly_business_metrics.payout_events + EXCLUDED.payout_events,
                    payout_to_revenue_ratio = CASE
                        WHEN (hourly_business_metrics.total_revenue + EXCLUDED.total_revenue) > 0
                        THEN (hourly_business_metrics.total_payouts + EXCLUDED.total_payouts) /
                             (hourly_business_metrics.total_revenue + EXCLUDED.total_revenue)
                        ELSE NULL
                    END,
                    active_users = hourly_business_metrics.active_users + EXCLUDED.active_users,
                    new_sessions = hourly_business_metrics.new_sessions + EXCLUDED.new_sessions,
                    total_logins = hourly_business_metrics.total_logins + EXCLUDED.total_logins,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                hour,
                stats.total_revenue,
                stats.ticket_count,
                avg_ticket_value,
                stats.total_payouts,
                stats.payout_events,
                payout_ratio,
                active_user_count,
                new_session_count,
                stats.total_logins
            ))

    except Exception:
        logging.exception("Failed to flush business metrics")

# Consumer Runner
def main():
    start_http_server(9090, addr="0.0.0.0")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    conn = psycopg2.connect(PG_CONN)
    cursor = conn.cursor()
    logging.info("Prometheus metrics server started on port 9090")
    logging.info("Kafka consumer started...")

    for message in consumer:
        try:
            event = message.value
            if validate_event(event):
                if ingest_event_raw(cursor, event):
                    process_event(event, business_stats)
                    EVENTS_RECEIVED.labels(status='success').inc()
                conn.commit()
            else:
                EVENTS_RECEIVED.labels(status='failure').inc()

        except Exception as e:
            EVENTS_RECEIVED.labels(status='failure').inc()
            logging.exception("Unexpected error while processing message")

        if int(time.time()) % 60 == 0:  # periodic flush
            flush_business_metrics(cursor, business_stats)
            conn.commit()
            business_stats.clear()

if __name__ == "__main__":
    main()