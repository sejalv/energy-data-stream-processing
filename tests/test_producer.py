# tests/test_producer.py
import json
import pytest
from unittest.mock import patch, MagicMock
from producer import load_events, validate_event, send_event

def test_load_events(tmp_path):
    sample = {
        "event_type": "view_tariffs",
        "event_time": "2025-06-01T02:04:33.033906",
        "payload": {
            "customer_id": "CUST0023",
            "session_id": 3703,
            "channel": "web_portal",
            "tariff_type": "basic"
        }
    }
    file = tmp_path / "temp_events.jsonl"
    errorfile = tmp_path / "error_file.log"
    file.write_text(json.dumps(sample) + "\n")
    events = list(load_events(file, errorfile))
    assert [e[0] for e in events] == [sample]

def test_read_malformed_events(tmp_path, caplog):
    sample = 'not a valid json\n'
    file = tmp_path / "malformed.jsonl"
    errorfile = tmp_path / "error_file.log"
    file.write_text(f'{sample}\n')
    with caplog.at_level("ERROR"):
        events = list(load_events(file, errorfile))

    assert events == []
    assert any("Malformed JSON skipped" in message for message in caplog.text.splitlines())

def test_validate_event_missing_keys():
    event = {
        "event_type": "login",
    }
    is_valid, reason = validate_event(event)
    assert is_valid is False
    assert "Missing required keys" in reason

def test_validate_event_empty_event_type():
    event = {
        "event_type": "",
        "event_time": "2025-06-01T23:18:33.033906",
        "payload": {}
    }
    is_valid, reason = validate_event(event)
    assert is_valid is False
    assert "event_type or payload is empty" in reason

def test_validate_event_valid():
    event = {
        "event_type": "user_login",
        "event_time": "2025-06-01T23:18:33.033906",
        "payload": {
            "customer_id": "CUST0026",
            "session_id": 3438,
            "channel": "web_portal"
        }
    }
    is_valid, reason = validate_event(event)
    assert is_valid is True
    assert reason == ""

@patch("producer.KafkaProducer")
def test_send_event(mock_kafka_producer):
    """Test sending event to Kafka"""
    mock_producer = MagicMock()
    event = {
        "event_type": "user_login",
        "event_time": "2025-06-01T23:18:33.033906",
        "payload": {
            "customer_id": "CUST0026",
            "session_id": 3438,
            "channel": "web_portal"
        }
    }
    send_event(mock_producer, "test_topic", event)
    mock_producer.send.assert_called_once_with("test_topic", event)