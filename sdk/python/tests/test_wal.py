"""Tests for WAL duplicate-send prevention and flush semantics."""

from unittest.mock import MagicMock, patch
import tempfile

from fluxmeter.client import FluxMeter
from fluxmeter.wal import WriteAheadLog


def test_wal_single_send_path():
    """Reading from offset after advance returns no duplicate events."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal = WriteAheadLog(path=tmpdir, flush_interval_sec=0.1)
        wal.append({"eventId": "evt-1", "customerId": "c1", "modelId": "gpt-4o"})
        wal.append({"eventId": "evt-2", "customerId": "c1", "modelId": "gpt-4o"})

        f = wal.pending_files()[0]
        evt1, off1 = wal.read_next_event_from_offset(f, 0)
        assert evt1["eventId"] == "evt-1"
        wal.advance_send_offset(f, off1)
        evt2, off2 = wal.read_next_event_from_offset(f, off1)
        assert evt2["eventId"] == "evt-2"
        wal.advance_send_offset(f, off2)
        evt3, _ = wal.read_next_event_from_offset(f, off2)
        assert evt3 is None


def test_wal_enabled_no_immediate_kafka():
    """With WAL on, _send does not call produce directly."""
    with patch("confluent_kafka.Producer") as mock_cls:
        mock_producer = MagicMock()
        mock_cls.return_value = mock_producer
        with tempfile.TemporaryDirectory() as tmpdir:
            meter = FluxMeter(
                kafka_brokers="localhost:9094",
                wal_enabled=True,
                wal_path=tmpdir,
            )
            meter.track("cust_1", "gpt-4o", input_tokens=10, output_tokens=5)
            assert not mock_producer.produce.called


def test_flush_drains_wal_before_close():
    """flush() sends WAL events synchronously before closing."""
    with patch("confluent_kafka.Producer") as mock_cls:
        mock_producer = MagicMock()
        mock_cls.return_value = mock_producer
        with tempfile.TemporaryDirectory() as tmpdir:
            meter = FluxMeter(
                kafka_brokers="localhost:9094",
                wal_enabled=True,
                wal_path=tmpdir,
            )
            meter.track("cust_1", "gpt-4o", input_tokens=1, output_tokens=1)
            meter.flush(timeout=5.0)
            assert mock_producer.produce.call_count == 1
            mock_producer.flush.assert_called()


def test_partial_send_advances_one_event_at_a_time():
    """First event ack advances offset; second event remains for retry."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal = WriteAheadLog(path=tmpdir)
        f = wal._current_file
        wal.append({"eventId": "a", "customerId": "c1"})
        wal.append({"eventId": "b", "customerId": "c1"})

        _, off1 = wal.read_next_event_from_offset(f, 0)
        wal.advance_send_offset(f, off1)
        evt_b, _ = wal.read_next_event_from_offset(f, off1)
        assert evt_b["eventId"] == "b"
