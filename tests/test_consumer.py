import pytest
from datetime import datetime
from consumer import validate_event, process_event, flush_business_metrics, HourlyStats
from unittest.mock import MagicMock
from collections import defaultdict

@pytest.fixture
def valid_event():
    return {
        "event_type": "ticket_purchased",
        "event_time": "2025-06-02T12:34:56Z",
        "payload": {
            "user_id": "user123",
            "session_id": 456,
            "amount": 50.0
        }
    }

@pytest.fixture
def business_stats():
    return defaultdict(HourlyStats)


def test_validate_event_valid(valid_event):
    assert validate_event(valid_event) is True

def test_validate_event_missing_payload_fields():
    event = {
        "event_type": "login",
        "event_time": "2025-06-02T10:00:00Z",
        "payload": {
            "user_id": "abc"
        }
    }
    assert validate_event(event) is False

def test_process_claim_reward(business_stats):
    event = {
        "event_type": "claim_reward",
        "event_time": "2025-06-02T14:10:00Z",
        "payload": {
            "user_id": "userX",
            "session_id": 123,
            "amount": 20.0
        }
    }
    process_event(event, business_stats)
    hour = datetime.fromisoformat("2025-06-02T14:00:00+00:00")
    stats = business_stats[hour]
    assert stats.total_payouts == 20.0
    assert stats.payout_events == 1

def test_flush_business_metrics_writes_to_db():
    stats = HourlyStats()
    stats.total_revenue = 100
    stats.ticket_count = 2
    stats.total_payouts = 50
    stats.payout_events = 1
    stats.active_users = {"u1", "u2"}
    stats.new_sessions = {"s1"}
    stats.total_logins = 5

    hour = datetime(2025, 6, 2, 10, 0, 0)
    business_stats = {hour: stats}

    cursor = MagicMock()
    flush_business_metrics(cursor, business_stats)

    assert cursor.execute.called
    # assert business_stats == {}
