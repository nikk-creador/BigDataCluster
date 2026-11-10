import json
import sqlite3
import unittest

from wikimedia_live_ingest import iter_sse_messages, normalize_recent_change
from wikimedia_stream_consumer import consume_batch, create_tables


class FakeResponse:
    def __init__(self, lines):
        self.lines = lines

    def iter_lines(self, decode_unicode=False):
        self.decode_unicode = decode_unicode
        return iter(self.lines)


class FakeMessage:
    topic = "wikimedia.recentchange.v1"
    partition = 0

    def __init__(self, offset, value):
        self.offset = offset
        self.value = value


class FakeConsumer:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


class WikimediaPipelineTests(unittest.TestCase):
    def test_sse_parser_retains_event_id_for_following_message(self):
        response = FakeResponse(
            [
                "id: event-1",
                'data: {"value":1}',
                "",
                'data: {"value":2}',
                "",
            ]
        )

        self.assertEqual(
            list(iter_sse_messages(response)),
            [
                ("event-1", '{"value":1}'),
                ("event-1", '{"value":2}'),
            ],
        )

    def test_normalizer_uses_schema_fields_and_omits_editor_details(self):
        event = normalize_recent_change(
            {
                "meta": {
                    "id": "change-1",
                    "dt": "2026-11-10T12:00:00Z",
                    "stream": "mediawiki.recentchange",
                },
                "wiki": "enwiki",
                "title": "Live page / title",
                "type": "edit",
                "namespace": 0,
                "server_url": "https://en.wikipedia.org",
                "bot": False,
                "minor": True,
                "length": {"old": 100, "new": 125},
                "revision": {"old": 3, "new": 4},
                "user": "not stored",
                "comment": "not stored",
            },
            "sse-id",
        )

        self.assertEqual(event["event_id"], "change-1")
        self.assertEqual(event["length_delta"], 25)
        self.assertEqual(
            event["page_url"],
            "https://en.wikipedia.org/wiki/Live_page_/_title",
        )
        self.assertNotIn("user", event)
        self.assertNotIn("comment", event)

    def test_consumer_deduplicates_and_records_invalid_messages(self):
        connection = sqlite3.connect(":memory:")
        create_tables(connection)
        event = {
            "event_type": "recent_change",
            "event_id": "change-1",
            "occurred_at": "2026-11-10T12:00:00Z",
            "received_at": "2026-11-10T12:00:01Z",
            "wiki": "enwiki",
            "title": "A page",
            "change_type": "edit",
        }
        message = json.dumps(event).encode("utf-8")
        consumer = FakeConsumer()
        result = consume_batch(
            connection,
            consumer,
            [
                FakeMessage(1, message),
                FakeMessage(2, message),
                FakeMessage(3, b"\xff"),
            ],
        )

        self.assertEqual(result, (3, 1, 1, 1))
        self.assertEqual(consumer.commits, 1)
        self.assertEqual(
            connection.execute("SELECT COUNT(*) FROM recent_changes").fetchone()[0],
            1,
        )
        state = dict(connection.execute("SELECT key, value FROM consumer_state"))
        self.assertEqual(state["recent_changes_consumed"], "1")
        self.assertEqual(state["duplicate_events"], "1")
        self.assertEqual(state["invalid_events"], "1")
        self.assertEqual(
            connection.execute("SELECT COUNT(*) FROM invalid_events").fetchone()[0],
            1,
        )
        connection.close()


if __name__ == "__main__":
    unittest.main()
