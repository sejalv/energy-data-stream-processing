# consumer.py
import os
import time
import json
import logging
import psycopg2
import argparse
import threading
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, Set, Tuple, Any, List
from collections import defaultdict
from dataclasses import dataclass, field
from kafka import KafkaConsumer
from psycopg2 import sql, pool
from psycopg2.extras import execute_batch
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Config
TOPIC = os.getenv("TOPIC", "energy_events")
GROUP_ID = os.getenv("GROUP_ID", "aggregator_group")
KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
PG_CONN = os.getenv("POSTGRES_CONN", f"dbname={os.getenv('POSTGRES_DB')} user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')} host={os.getenv('POSTGRES_HOST')} port=5432")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
COMMIT_INTERVAL = int(os.getenv("COMMIT_INTERVAL", "25"))  # Commit every N events
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "30"))  # Flush metrics

# Prometheus Metrics
EVENTS_PROCESSED = Counter('consumer_events_processed_total', 'Total events processed by consumer', ['status'])
PROCESSING_LATENCY = Histogram('consumer_event_processing_latency', 'Event processing latency in seconds',  buckets=[0.001, 0.01, 0.1, 1.0, 5.0])
DB_OPERATIONS = Counter('db_operations_total', 'Database operations', ['operation', 'status'])
MEMORY_USAGE = Gauge('memory_usage_mb', 'Memory usage in MB')

# Database connection pool metrics
DB_POOL_ACTIVE = Gauge('db_pool_active_connections', 'Active database connections')
DB_POOL_IDLE = Gauge('db_pool_idle_connections', 'Idle database connections')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

shutdown_requested = False  # Global shutdown flag

def signal_handler(signum, frame):
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def create_parser():
    parser = argparse.ArgumentParser(description="Energy domain-specific Kafka consumer")
    parser.add_argument("--topic", type=str, default=TOPIC, help="Kafka topic to consume from")
    parser.add_argument("--group_id", type=str, default=GROUP_ID, help="Consumer group ID")
    parser.add_argument("--error_file", type=str, default="data/consumer_errors.log", help="Path to error log")

    parser.add_argument("--batch_size", type=int, default=BATCH_SIZE, help="Batch size for processing events")
    parser.add_argument("--pool_size", type=int, default=5, help="Database connection pool size")

    return parser


def validate_numeric_field(value: Any) -> Tuple[bool, float]:
    """Simple numeric validation and conversion"""
    if value is None:
        return True, 0.0
    try:
        float_val = float(value)
        if float_val < 0:
            return False, 0.0
        return True, float_val
    except (ValueError, TypeError):
        return False, 0.0

def log_invalid_event(event: Dict[str, Any], reason: str, log_path: str):
    EVENTS_PROCESSED.labels(status='failure').inc()
    with open(log_path, "a") as f:
        f.write(f"{reason}\n{json.dumps(event)}\n")

@dataclass
class HourlyStats:
    tariff_switches: int = 0
    total_switch_revenue: float = 0.0
    incentive_claims: int = 0
    total_incentive_payouts: float = 0.0
    green_tariff_switches: int = 0
    active_customers: Set[str] = field(default_factory=set)
    new_sessions: Set[int] = field(default_factory=set)
    total_logins: int = 0
    total_energy_consumed: float = 0.0
    energy_by_customer: Dict[str, float] = field(default_factory=lambda: defaultdict(float))
    peak_hour_usage: float = 0.0
    total_payments: float = 0.0
    payment_events: int = 0

# Thread-safe business stats with lock
business_stats: Dict[datetime, HourlyStats] = defaultdict(HourlyStats)
stats_lock = threading.Lock()

# Required payload fields per event type
REQUIRED_PAYLOAD_FIELDS = {
    "view_tariffs": {"customer_id", "session_id", "channel", "tariff_type"},
    "user_login": {"customer_id", "session_id", "channel"},
    "user_logout": {"customer_id", "session_id", "channel"},
    "tariff_switch": {"customer_id", "session_id", "channel", "tariff_type"},
    "energy_consumed": {"customer_id", "session_id", "channel", "energy_consumed"},
    "incentive_claim": {"customer_id", "session_id", "channel", "tariff_type"},
    "bill_payment": {"customer_id", "session_id", "channel", "payment_amount"}
}

def validate_event(event: Dict) -> Tuple[bool, str]:
    try:
        datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
    except (ValueError, AttributeError, KeyError) as e:
        return False, f"Invalid event_time format: {str(e)}"

    required_fields = REQUIRED_PAYLOAD_FIELDS.get(event.get("event_type"), set())
    if not required_fields.issubset(event.get("payload", {})):
        missing = required_fields - set(event.get("payload", {}).keys())
        return False, f"Missing payload fields: {missing}"

    return True, ""

def log_invalid(event: Dict, reason: str, path: str):
    with open(path, "a") as f:
        f.write(f"{reason}\n{json.dumps(event)}\n")
    EVENTS_PROCESSED.labels(status="failure").inc()


class DatabaseManager:
    def __init__(self, connection_string: str, pool_size: int = 5):
        self.connection_string = connection_string
        self.pool_size = pool_size
        self.connection_pool = None
        self._init_pool()

    def _init_pool(self):
        """Initialize connection pool with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    1, self.pool_size, self.connection_string
                )
                logger.info(f"Database connection pool initialized with {self.pool_size} connections")
                return
            except psycopg2.OperationalError as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to initialize DB pool after {max_retries} attempts: {e}")
                    raise
                logger.warning(f"DB pool init failed (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)

    def get_connection(self):
        """Get connection from pool with retry logic"""
        if not self.connection_pool:
            raise Exception("Connection pool not initialized")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = self.connection_pool.getconn()
                if conn.closed:
                    self.connection_pool.putconn(conn, close=True)
                    continue
                return conn
            except psycopg2.OperationalError as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Failed to get DB connection (attempt {attempt + 1}): {e}")
                time.sleep(0.5)

    def return_connection(self, conn, close=False):
        """Return connection to pool"""
        if self.connection_pool and conn:
            self.connection_pool.putconn(conn, close=close)

    def close_pool(self):
        """Close all connections in pool"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Database connection pool closed")


def ingest_raw_batch(cursor, events: List[Dict]) -> int:
    """Batch insert events for better performance"""
    if not events:
        return 0

    try:
        data = []
        for event in events:
            payload = event["payload"]
            energy_consumed = None
            payment_amount = None

            if "energy_consumed" in payload:
                valid, energy_consumed = validate_numeric_field(payload["energy_consumed"])
                if not valid:
                    energy_consumed = None

            if "payment_amount" in payload:
                valid, payment_amount = validate_numeric_field(payload["payment_amount"])
                if not valid:
                    payment_amount = None

            data.append((
                payload["customer_id"],
                event["event_type"],
                event["event_time"],
                energy_consumed,
                payment_amount,
                payload["session_id"],
                payload.get("tariff_type"),
                payload.get("channel")
            ))

        execute_batch(
            cursor,
            """
            INSERT INTO events (
                customer_id, event_type, event_time, energy_consumed,
                payment_amount, session_id, tariff_type, channel
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            data,
            page_size=BATCH_SIZE
        )

        DB_OPERATIONS.labels(operation='insert', status='success').inc(len(events))
        return len(events)

    except Exception as e:
        logger.error(f"Batch insert failed: {e}")
        DB_OPERATIONS.labels(operation='insert', status='failure').inc(len(events))
        return 0


@PROCESSING_LATENCY.time()
def process_event(event: Dict):
    try:
        dt = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
        hour = dt.replace(minute=0, second=0, microsecond=0)

        event_type = event["event_type"]
        payload = event["payload"]
        customer_id = payload["customer_id"]
        session_id = int(payload["session_id"])

        with stats_lock:
            stats = business_stats[hour]

            if event_type == "tariff_switch":
                stats.tariff_switches += 1
                if "payment_amount" in payload:
                    valid, amount = validate_numeric_field(payload["payment_amount"])
                    if valid:
                        stats.total_switch_revenue += amount

                if payload.get("tariff_type") == "green":
                    stats.green_tariff_switches += 1

            elif event_type == "incentive_claim":
                stats.incentive_claims += 1
                if "payment_amount" in payload:
                    valid, payout = validate_numeric_field(payload["payment_amount"])
                    if valid:
                        stats.total_incentive_payouts += payout

            elif event_type == "user_login":
                stats.total_logins += 1
                stats.new_sessions.add(session_id)

            elif event_type == "energy_consumed":
                valid, energy = validate_numeric_field(payload["energy_consumed"])
                if valid:
                    stats.total_energy_consumed += energy
                    stats.energy_by_customer[customer_id] += energy
                    if energy > stats.peak_hour_usage:
                        stats.peak_hour_usage = energy

            elif event_type == "bill_payment":
                valid, amount = validate_numeric_field(payload["payment_amount"])
                if valid:
                    stats.total_payments += amount
                    stats.payment_events += 1

            stats.active_customers.add(customer_id)

    except Exception as e:
        logger.warning(f"Processing error for event {event.get('event_type', 'unknown')}: {e}")
        EVENTS_PROCESSED.labels(status="failure").inc()


def flush_business_metrics(db_manager: DatabaseManager):
    """Flush business metrics with better error handling and cleanup"""
    if not business_stats:
        return

    conn = None
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()

        with stats_lock:
            stats_to_flush = dict(business_stats)

        for hour, stats in stats_to_flush.items():
            try:
                active_customer_count = len(stats.active_customers)
                new_session_count = len(stats.new_sessions)
                avg_switch_value = (stats.total_switch_revenue / stats.tariff_switches
                                    if stats.tariff_switches > 0 else 0)
                avg_consumption = (stats.total_energy_consumed / active_customer_count
                                   if active_customer_count > 0 else 0)
                avg_payment = (stats.total_payments / stats.payment_events
                               if stats.payment_events > 0 else 0)
                peak_hour_usage = stats.peak_hour_usage

                cursor.execute("""
                    INSERT INTO hourly_business_metrics (
                        hour, tariff_switches, total_switch_revenue, avg_switch_value,
                        incentive_claims, total_incentive_payouts, green_tariff_switches,
                        active_customers, new_sessions, total_logins,
                        total_energy_consumed, avg_consumption_per_customer, peak_hour_usage,
                        total_payments, payment_events, avg_payment_amount
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (hour) DO UPDATE SET
                        tariff_switches = hourly_business_metrics.tariff_switches + EXCLUDED.tariff_switches,
                        total_switch_revenue = hourly_business_metrics.total_switch_revenue + EXCLUDED.total_switch_revenue,
                        avg_switch_value = CASE
                            WHEN (hourly_business_metrics.tariff_switches + EXCLUDED.tariff_switches) > 0 THEN
                                (hourly_business_metrics.total_switch_revenue + EXCLUDED.total_switch_revenue) /
                                (hourly_business_metrics.tariff_switches + EXCLUDED.tariff_switches)
                            ELSE 0
                        END,
                        incentive_claims = hourly_business_metrics.incentive_claims + EXCLUDED.incentive_claims,
                        total_incentive_payouts = hourly_business_metrics.total_incentive_payouts + EXCLUDED.total_incentive_payouts,
                        green_tariff_switches = hourly_business_metrics.green_tariff_switches + EXCLUDED.green_tariff_switches,
                        active_customers = hourly_business_metrics.active_customers + EXCLUDED.active_customers,
                        new_sessions = hourly_business_metrics.new_sessions + EXCLUDED.new_sessions,
                        total_logins = hourly_business_metrics.total_logins + EXCLUDED.total_logins,
                        total_energy_consumed = hourly_business_metrics.total_energy_consumed + EXCLUDED.total_energy_consumed,
                        avg_consumption_per_customer = CASE
                            WHEN (hourly_business_metrics.active_customers + EXCLUDED.active_customers) > 0 THEN
                                (hourly_business_metrics.total_energy_consumed + EXCLUDED.total_energy_consumed) /
                                (hourly_business_metrics.active_customers + EXCLUDED.active_customers)
                            ELSE 0
                        END,
                        peak_hour_usage = EXCLUDED.peak_hour_usage,
                        total_payments = hourly_business_metrics.total_payments + EXCLUDED.total_payments,
                        payment_events = hourly_business_metrics.payment_events + EXCLUDED.payment_events,
                        avg_payment_amount = CASE
                            WHEN (hourly_business_metrics.payment_events + EXCLUDED.payment_events) > 0 THEN
                                (hourly_business_metrics.total_payments + EXCLUDED.total_payments) /
                                (hourly_business_metrics.payment_events + EXCLUDED.payment_events)
                            ELSE 0
                        END,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    hour, stats.tariff_switches, stats.total_switch_revenue, avg_switch_value,
                    stats.incentive_claims, stats.total_incentive_payouts, stats.green_tariff_switches,
                    active_customer_count, new_session_count, stats.total_logins,
                    stats.total_energy_consumed, avg_consumption, peak_hour_usage,
                    stats.total_payments, stats.payment_events, avg_payment
                ))

            except Exception as e:
                logger.error(f"Failed to flush metrics for hour {hour}: {e}")
                continue

        conn.commit()
        DB_OPERATIONS.labels(operation='flush_metrics', status='success').inc()

        # Clean up old stats (keep last 24 hours)
        with stats_lock:
            cutoff = datetime.now() - timedelta(hours=24)
            old_keys = [k for k in business_stats.keys() if k < cutoff]
            for key in old_keys:
                del business_stats[key]

        logger.info(f"Flushed metrics for {len(stats_to_flush)} hours, cleaned up {len(old_keys)} old entries")

    except Exception as e:
        logger.error(f"Failed to flush business metrics: {e}")
        DB_OPERATIONS.labels(operation='flush_metrics', status='failure').inc()
    finally:
        if conn:
            cursor.close()
            db_manager.return_connection(conn)


def periodic_flush_worker(db_manager: DatabaseManager):
    """Background worker to periodically flush metrics"""
    logger.info(f"Starting periodic flush worker (interval: {FLUSH_INTERVAL}s)")

    while not shutdown_requested:
        try:
            time.sleep(FLUSH_INTERVAL)
            if not shutdown_requested:
                flush_business_metrics(db_manager)

                # Update memory usage metric
                try:
                    import psutil
                    process = psutil.Process()
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    MEMORY_USAGE.set(memory_mb)
                except ImportError:
                    pass  # psutil not available

        except Exception as e:
            logger.error(f"Error in periodic flush worker: {e}")
            time.sleep(10)  # Wait before retrying


def process_event_batch(events: List[Dict], db_manager: DatabaseManager, error_file: str) -> int:
    """Process a batch of events efficiently"""
    if not events:
        return 0

    valid_events = []

    # Validate all events first
    for event in events:
        valid, reason = validate_event(event)
        if valid:
            valid_events.append(event)
        else:
            log_invalid_event(event, reason, error_file)
            PROCESSING_LATENCY.labels(status='failure').inc()

    if not valid_events:
        return 0

    # Database operations
    conn = None
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()

        # Batch insert raw events
        inserted_count = ingest_raw_batch(cursor, valid_events)

        if inserted_count > 0:
            for event in valid_events:
                process_event(event)
                EVENTS_PROCESSED.labels(status='success').inc()

            conn.commit()
            return inserted_count
        else:
            return 0

    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        if conn:
            conn.rollback()

        for event in valid_events:
            EVENTS_PROCESSED.labels(status='failure').inc()
        return 0

    finally:
        if conn:
            cursor.close()
            db_manager.return_connection(conn)

def main():
    global shutdown_requested

    parser = create_parser()
    args = parser.parse_args()

    start_http_server(9090, addr="0.0.0.0")     # Prometheus metrics server
    logger.info("Prometheus metrics server started on port 9090")

    db_manager = DatabaseManager(PG_CONN, args.pool_size)

    flush_thread = threading.Thread(target=periodic_flush_worker, args=(db_manager,), daemon=True)  # Background flush worker
    flush_thread.start()

    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=KAFKA_BROKER,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # Manual commit for better control
        max_poll_records=args.batch_size,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    logger.info(f"Kafka consumer started for topic '{args.topic}' with group '{args.group_id}'")
    logger.info(f"Batch size: {args.batch_size}, Commit interval: {COMMIT_INTERVAL}")

    try:
        event_batch = []
        events_since_commit = 0

        for msg in consumer:
            if shutdown_requested:
                break

            logging.info(f"Listening to: {msg.value}")
            event_batch.append(msg.value)

            # Process & commit in batches
            if len(event_batch) >= args.batch_size:
                processed = process_event_batch(event_batch, db_manager, args.error_file)
                events_since_commit += processed
                event_batch = []

                if events_since_commit >= COMMIT_INTERVAL:
                    consumer.commit()
                    events_since_commit = 0
                    logger.debug(f"Committed after {COMMIT_INTERVAL} events")

        # Process remaining events
        if event_batch:
            process_event_batch(event_batch, db_manager, args.error_file)
            consumer.commit()

    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        logger.info("Shutting down gracefully...")

        try:
            flush_business_metrics(db_manager)
            logger.info("Final metrics flush completed")
        except Exception as e:
            logger.error(f"Final flush failed: {e}")

        # Cleanup
        if 'consumer' in locals():
            consumer.close()
        db_manager.close_pool()

        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()