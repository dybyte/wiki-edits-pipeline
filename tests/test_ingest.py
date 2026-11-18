import datetime as dt
import json
import unittest
from unittest.mock import patch

from ingest import consumer, producer


class FakeMessage:
    def __init__(self, payload: dict, partition: int = 2, offset: int = 41):
        self._value = json.dumps(payload).encode()
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class RecordingClient:
    def __init__(self, events: list[str], error: Exception | None = None):
        self.events = events
        self.error = error

    def insert(self, table, batch, column_names):
        self.events.append("insert")
        if self.error:
            raise self.error


class RecordingConsumer:
    def __init__(self, events: list[str]):
        self.events = events

    def commit(self, asynchronous):
        self.events.append("commit")


class ConsumerTest(unittest.TestCase):
    def test_to_row_extracts_event_and_kafka_position(self):
        payload = {"meta": {"id": "event-1", "dt": "2026-11-18T12:34:56+00:00"}}

        row = consumer.to_row(FakeMessage(payload))

        self.assertEqual("event-1", row[0])
        self.assertEqual(dt.datetime(2026, 11, 18, 12, 34, 56, tzinfo=dt.timezone.utc), row[1])
        self.assertEqual(2, row[3])
        self.assertEqual(41, row[4])

    def test_flush_inserts_before_committing_offset(self):
        events: list[str] = []

        consumer.flush(RecordingClient(events), RecordingConsumer(events), [["row"]])

        self.assertEqual(["insert", "commit"], events)

    def test_flush_does_not_commit_when_insert_fails(self):
        events: list[str] = []

        with self.assertRaisesRegex(RuntimeError, "warehouse unavailable"):
            consumer.flush(
                RecordingClient(events, RuntimeError("warehouse unavailable")),
                RecordingConsumer(events),
                [["row"]],
            )

        self.assertEqual(["insert"], events)

    def test_fault_injection_exits_between_insert_and_commit(self):
        class InjectedCrash(Exception):
            pass

        events: list[str] = []
        with (
            patch.dict("os.environ", {consumer.CRASH_AFTER_INSERT_ENV: "1"}),
            patch("ingest.consumer.os._exit", side_effect=InjectedCrash),
            self.assertRaises(InjectedCrash),
        ):
            consumer.flush(RecordingClient(events), RecordingConsumer(events), [["row"]])

        self.assertEqual(["insert"], events)


class ProducerTest(unittest.TestCase):
    def test_checkpoint_saves_only_after_all_messages_are_delivered(self):
        class FlushedProducer:
            def flush(self, timeout):
                return 0

        with patch("ingest.producer.save_checkpoint") as save:
            producer.checkpoint(FlushedProducer(), "sse-7", [])

        save.assert_called_once_with("sse-7")

    def test_checkpoint_does_not_advance_when_messages_remain(self):
        class BackloggedProducer:
            def flush(self, timeout):
                return 3

        with (
            patch("ingest.producer.save_checkpoint") as save,
            self.assertRaises(RuntimeError),
        ):
            producer.checkpoint(BackloggedProducer(), "sse-7", [])

        save.assert_not_called()


if __name__ == "__main__":
    unittest.main()
