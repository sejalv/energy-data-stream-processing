import pytest
from consumer import validate_event, validate_numeric_field, process_event, HourlyStats, business_stats
from datetime import datetime
from collections import defaultdict
import threading

@pytest.fixture
def sample_valid_event():
    return {
        "event_type": "tariff_switch",
        "event_time": "2025-06-04T12:00:00Z",
        "payload": {
            "customer_id": "cust123",
            "session_id": "101",
            "channel": "web",
            "tariff_type": "green",
            "payment_amount": "20.5"
        }
    }

@pytest.fixture
def sample_invalid_event():
    return {
        "event_type": "tariff_switch",
        "event_time": "invalid-date",
        "payload": {
            "customer_id": "cust123",
            "session_id": "101",
            "channel": "web",
        }
    }

def test_validate_event_valid(sample_valid_event):
    is_valid, reason = validate_event(sample_valid_event)
    assert is_valid
    assert reason == ""

def test_validate_event_invalid_time(sample_invalid_event):
    is_valid, reason = validate_event(sample_invalid_event)
    assert not is_valid
    assert "Invalid event_time format" in reason

def test_validate_event_missing_fields(sample_invalid_event):
    sample_invalid_event["event_time"] = "2025-06-04T12:00:00Z"  # Fix time
    is_valid, reason = validate_event(sample_invalid_event)
    assert not is_valid
    assert "Missing payload fields" in reason

def test_validate_numeric_field_negative():
    valid, val = validate_numeric_field("-5.5")
    assert not valid
    assert val == 0.0

def test_process_event_tariff_switch(sample_valid_event):
    business_stats.clear()  # Reset shared state
    event = sample_valid_event
    process_event(event)
    hour = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00")).replace(minute=0, second=0, microsecond=0)
    stats = business_stats[hour]
    assert stats.tariff_switches == 1
    assert stats.total_switch_revenue == 20.5
    assert stats.green_tariff_switches == 1
    assert "cust123" in stats.active_customers

def test_process_multiple_events_same_customer():
    business_stats.clear()
    event1 = {
        "event_type": "user_login",
        "event_time": "2025-06-04T10:00:00Z",
        "payload": {
            "customer_id": "cust456",
            "session_id": "201",
            "channel": "app"
        }
    }
    event2 = {
        "event_type": "user_login",
        "event_time": "2025-06-04T10:15:00Z",
        "payload": {
            "customer_id": "cust456",
            "session_id": "202",
            "channel": "web"
        }
    }
    process_event(event1)
    process_event(event2)
    hour = datetime.fromisoformat("2025-06-04T10:00:00+00:00")
    stats = business_stats[hour]
    assert stats.total_logins == 2
    assert len(stats.new_sessions) == 2
    assert "cust456" in stats.active_customers
