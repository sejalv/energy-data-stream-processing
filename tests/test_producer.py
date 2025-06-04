# tests/test_producer.py
import json
import pytest
from unittest.mock import patch, MagicMock
from producer import load_events, validate_event, send_event

def test_load_events(tmp_path):
    sample = {"user_id": "user_1", "event_type": "purchase"}
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
        "user_id": "abc",
        "session_id": "def",
        }
    is_valid, _ = validate_event(event)
    assert is_valid is False

@patch("producer.KafkaProducer")
def test_send_event(mock_kafka_producer):
    mock_producer = MagicMock()
    event = {"user_id": "user_2", "event_type": "login"}
    send_event(mock_producer, "test_topic", event)

    mock_producer.send.assert_called_once_with("test_topic", event)
