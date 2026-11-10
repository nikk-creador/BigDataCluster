"""Consume Wikimedia edit events from Kafka into a local SQLite history."""
import json
import logging
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"), override=True)

DEFAULT_TOPIC = "wikimedia.recentchange.v1"
LOGGER = logging.getLogger("wikimedia-kafka-consumer")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def create_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS recent_changes (
            event_id TEXT PRIMARY KEY,
            occurred_at TEXT NOT NULL,
            received_at TEXT NOT NULL,
            stream TEXT NOT NULL,
            wiki TEXT NOT NULL,
            server_name TEXT NOT NULL,
            title TEXT NOT NULL,
            page_url TEXT NOT NULL,
            change_type TEXT NOT NULL,
            namespace INTEGER,
            bot INTEGER NOT NULL,
            minor INTEGER NOT NULL,
            length_old INTEGER,
            length_new INTEGER,
            length_delta INTEGER,
            revision_old INTEGER,
            revision_new INTEGER,
            message_bytes INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_recent_changes_occurred
            ON recent_changes(occurred_at);
        CREATE INDEX IF NOT EXISTS idx_recent_changes_wiki_time
            ON recent_changes(wiki, occurred_at);
        CREATE INDEX IF NOT EXISTS idx_recent_changes_type_time
            ON recent_changes(change_type, occurred_at);
        CREATE TABLE IF NOT EXISTS invalid_events (
            event_key TEXT PRIMARY KEY,
            received_at TEXT NOT NULL,
            reason TEXT NOT NULL,
            message_bytes INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS consumer_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )
    for key in (
        "events_consumed",
        "recent_changes_consumed",
        "duplicate_events",
        "invalid_events",
    ):
        connection.execute(
            "INSERT OR IGNORE INTO consumer_state(key, value) VALUES (?, '0')",
            (key,),
        )
    connection.commit()


def store_recent_change(
    connection: sqlite3.Connection, event: dict, message_bytes: int
) -> bool:
    event_id = event.get("event_id")
    occurred_at = event.get("occurred_at")
    wiki = event.get("wiki")
    title = event.get("title")
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("RecentChange event is missing its event_id")
    if not isinstance(occurred_at, str) or not occurred_at:
        raise ValueError(f"RecentChange event {event_id!r} has no timestamp")
    if not isinstance(wiki, str) or not wiki:
        raise ValueError(f"RecentChange event {event_id!r} has no wiki")
    if not isinstance(title, str) or not title:
        raise ValueError(
            f"RecentChange event {event_id!r} has no title"
        )
    try:
        datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(
            f"RecentChange event {event_id!r} has an invalid timestamp"
        ) from error
    for key in (
        "namespace",
        "length_old",
        "length_new",
        "length_delta",
        "revision_old",
        "revision_new",
    ):
        value = event.get(key)
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int)
        ):
            raise ValueError(
                f"RecentChange event {event_id!r} has invalid {key}"
            )
    for key in ("bot", "minor"):
        if key in event and not isinstance(event[key], bool):
            raise ValueError(f"RecentChange event {event_id!r} has invalid {key}")

    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO recent_changes (
            event_id, occurred_at, received_at, stream, wiki, server_name,
            title, page_url, change_type, namespace, bot, minor, length_old,
            length_new, length_delta, revision_old, revision_new, message_bytes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            occurred_at,
            str(event.get("received_at") or utc_now()),
            str(event.get("stream") or "mediawiki.recentchange"),
            wiki,
            str(event.get("server_name") or ""),
            title,
            str(event.get("page_url") or ""),
            str(event.get("change_type") or "unknown"),
            event.get("namespace"),
            int(bool(event.get("bot", False))),
            int(bool(event.get("minor", False))),
            event.get("length_old"),
            event.get("length_new"),
            event.get("length_delta"),
            event.get("revision_old"),
            event.get("revision_new"),
            message_bytes,
        ),
    )
    return cursor.rowcount == 1


def update_state(connection: sqlite3.Connection, key: str, value: object) -> None:
    connection.execute(
        "INSERT INTO consumer_state(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )


def record_invalid(
    connection: sqlite3.Connection, message, message_bytes: int, reason: str
) -> bool:
    event_key = (
        f"{getattr(message, 'topic', '')}:"
        f"{getattr(message, 'partition', '')}:"
        f"{getattr(message, 'offset', uuid.uuid4().hex)}"
    )
    cursor = connection.execute(
        "INSERT OR IGNORE INTO invalid_events "
        "(event_key, received_at, reason, message_bytes) VALUES (?, ?, ?, ?)",
        (event_key, utc_now(), reason[:500], message_bytes),
    )
    return cursor.rowcount == 1


def consume_batch(
    connection: sqlite3.Connection, consumer, messages: list
) -> tuple[int, int, int, int]:
    decoded: list[tuple[dict | None, int, str | None, object]] = []
    for message in messages:
        try:
            event = json.loads(message.value)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            decoded.append(
                (None, len(message.value), f"Invalid UTF-8 JSON: {error}", message)
            )
            continue
        if not isinstance(event, dict):
            decoded.append(
                (None, len(message.value), "Kafka event must be a JSON object", message)
            )
            continue
        decoded.append((event, len(message.value), None, message))

    recent_count = 0
    duplicate_count = 0
    invalid_count = 0
    try:
        connection.execute("BEGIN")
        for event, message_bytes, decode_error, message in decoded:
            if decode_error:
                invalid_count += int(
                    record_invalid(connection, message, message_bytes, decode_error)
                )
                continue
            if event is None:
                invalid_count += int(
                    record_invalid(
                        connection,
                        message,
                        message_bytes,
                        "Kafka event could not be decoded",
                    )
                )
                continue
            event_type = event.get("event_type")
            if event_type == "recent_change":
                try:
                    if store_recent_change(connection, event, message_bytes):
                        recent_count += 1
                        update_state(
                            connection, "last_event_received_at", utc_now()
                        )
                    else:
                        duplicate_count += 1
                except (TypeError, ValueError) as error:
                    invalid_count += int(
                        record_invalid(
                            connection, message, message_bytes, str(error)
                        )
                    )
            elif event_type == "stream_status":
                update_state(
                    connection,
                    "stream_status",
                    str(event.get("status") or "unknown"),
                )
                update_state(
                    connection,
                    "stream_status_at",
                    str(event.get("observed_at") or utc_now()),
                )
                update_state(
                    connection,
                    "stream_status_detail",
                    str(event.get("detail") or ""),
                )
            else:
                invalid_count += int(
                    record_invalid(
                        connection,
                        message,
                        message_bytes,
                        f"Unsupported Wikimedia event type: {event_type}",
                    )
                )

        update_state(connection, "last_consumed_at", utc_now())
        connection.execute(
            "UPDATE consumer_state SET value=CAST(value AS INTEGER)+? WHERE key='events_consumed'",
            (len(decoded),),
        )
        connection.execute(
            "UPDATE consumer_state SET value=CAST(value AS INTEGER)+? "
            "WHERE key='recent_changes_consumed'",
            (recent_count,),
        )
        connection.execute(
            "UPDATE consumer_state SET value=CAST(value AS INTEGER)+? "
            "WHERE key='duplicate_events'",
            (duplicate_count,),
        )
        connection.execute(
            "UPDATE consumer_state SET value=CAST(value AS INTEGER)+? "
            "WHERE key='invalid_events'",
            (invalid_count,),
        )
        connection.execute(
            "DELETE FROM recent_changes WHERE occurred_at < "
            "strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-7 days')"
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    consumer.commit()
    return len(decoded), recent_count, duplicate_count, invalid_count


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    )
    parser.add_argument(
        "--topic", default=os.getenv("WIKIMEDIA_KAFKA_TOPIC", DEFAULT_TOPIC)
    )
    parser.add_argument(
        "--group-id",
        default=os.getenv("WIKIMEDIA_KAFKA_GROUP", "wikimedia-dashboard-sqlite-v1"),
    )
    parser.add_argument(
        "--db",
        default=os.getenv(
            "WIKIMEDIA_DB",
            str(Path(__file__).with_name("wikimedia_live.db")),
        ),
    )
    args = parser.parse_args()
    if not args.bootstrap_servers or not args.bootstrap_servers.strip():
        parser.error("set KAFKA_BOOTSTRAP_SERVERS in .env or pass --bootstrap-servers")
    if not args.topic.strip():
        parser.error("--topic must not be empty")
    LOGGER.info(
        "Using Kafka bootstrap servers %s for topic %s",
        args.bootstrap_servers.strip(),
        args.topic.strip(),
    )

    try:
        from kafka import KafkaConsumer
    except ImportError as error:
        raise RuntimeError(
            "Kafka consumption requires kafka-python; install requirements.txt"
        ) from error

    database = Path(args.db).expanduser().resolve()
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database, timeout=30)
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA journal_mode = WAL")
    create_tables(connection)
    consumer = KafkaConsumer(
        args.topic.strip(),
        bootstrap_servers=[
            server.strip() for server in args.bootstrap_servers.split(",")
        ],
        group_id=args.group_id.strip(),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=1000,
    )
    LOGGER.info(
        "Consuming Wikimedia events from %s into %s",
        args.topic.strip(),
        database,
    )
    total = 0
    try:
        while True:
            polled = consumer.poll(timeout_ms=1000, max_records=1000)
            messages = [
                message for partition in polled.values() for message in partition
            ]
            if not messages:
                continue
            received, inserted, duplicates, invalid = consume_batch(
                connection, consumer, messages
            )
            total += received
            LOGGER.info(
                "Committed %s Kafka events (%s edits, %s duplicates, %s invalid; %s total this run)",
                received,
                inserted,
                duplicates,
                invalid,
                total,
            )
    except KeyboardInterrupt:
        LOGGER.info("Stopping after receiving %s Kafka events", total)
    finally:
        connection.close()
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    main()
