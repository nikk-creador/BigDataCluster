"""Consume Wikimedia's continuous RecentChange SSE stream and publish to Kafka."""
import json
import logging
import os
import random
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"), override=True)

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
DEFAULT_TOPIC = "wikimedia.recentchange.v1"
LOGGER = logging.getLogger("wikimedia-live-producer")
HEARTBEAT_SECONDS = 30


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def event_timestamp(data: dict, meta: dict) -> str:
    timestamp = meta.get("dt")
    if timestamp:
        return str(timestamp)
    unix_timestamp = data.get("timestamp")
    timestamp_seconds = optional_int(unix_timestamp)
    if timestamp_seconds is not None:
        try:
            return datetime.fromtimestamp(
                timestamp_seconds, timezone.utc
            ).isoformat()
        except (OSError, OverflowError, ValueError):
            return ""
    return ""


def optional_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, (float, str)):
        try:
            return int(value)
        except (ValueError, OverflowError):
            return None
    return None


def normalize_recent_change(data: dict, sse_id: str | None) -> dict:
    meta = data.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    length = data.get("length")
    if not isinstance(length, dict):
        length = {}
    revision = data.get("revision")
    if not isinstance(revision, dict):
        revision = {}

    wiki = str(data.get("wiki") or meta.get("domain") or "")
    title = str(data.get("title") or "")
    server_url = str(data.get("server_url") or "").rstrip("/")
    page_url = (
        f"{server_url}/wiki/{quote(title.replace(' ', '_'), safe='/():')}"
        if server_url and title
        else ""
    )
    old_length = optional_int(length.get("old"))
    new_length = optional_int(length.get("new"))
    event_id = str(meta.get("id") or sse_id or data.get("id") or uuid.uuid4().hex)

    return {
        "event_type": "recent_change",
        "event_id": event_id,
        "occurred_at": event_timestamp(data, meta),
        "received_at": utc_now(),
        "stream": str(meta.get("stream") or "mediawiki.recentchange"),
        "wiki": wiki,
        "server_name": str(data.get("server_name") or ""),
        "title": title,
        "page_url": page_url,
        "change_type": str(data.get("type") or "unknown"),
        "namespace": optional_int(data.get("namespace")),
        "bot": bool(data.get("bot", False)),
        "minor": bool(data.get("minor", False)),
        "length_old": old_length,
        "length_new": new_length,
        "length_delta": (
            new_length - old_length
            if old_length is not None and new_length is not None
            else None
        ),
        "revision_old": optional_int(revision.get("old")),
        "revision_new": optional_int(revision.get("new")),
    }


def publish(producer, topic: str, key: str, payload: dict) -> None:
    value = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    producer.send(topic, key=key.encode("utf-8"), value=value).get(timeout=60)


def init_resume_store(db_path: str) -> sqlite3.Connection:
    path = Path(db_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS producer_state "
        "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    connection.commit()
    return connection


def load_last_event_id(connection: sqlite3.Connection) -> str | None:
    row = connection.execute(
        "SELECT value FROM producer_state WHERE key = 'last_event_id'"
    ).fetchone()
    return str(row[0]) if row else None


def save_last_event_id(connection: sqlite3.Connection, event_id: str) -> None:
    connection.execute(
        "INSERT INTO producer_state(key, value) VALUES ('last_event_id', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (event_id,),
    )
    connection.commit()


def send_status(producer, topic: str, status: str, detail: str = "") -> None:
    publish(
        producer,
        topic,
        "wikimedia-stream-status",
        {
            "event_type": "stream_status",
            "status": status,
            "detail": detail[:500],
            "observed_at": utc_now(),
        },
    )


def start_heartbeat(producer, topic: str) -> tuple[threading.Event, threading.Thread]:
    stop = threading.Event()

    def beat() -> None:
        while not stop.wait(HEARTBEAT_SECONDS):
            try:
                send_status(producer, topic, "connected")
            except Exception:
                LOGGER.exception("Could not publish stream heartbeat")

    thread = threading.Thread(target=beat, name="kafka-heartbeat", daemon=True)
    thread.start()
    return stop, thread


def iter_sse_messages(response):
    """Yield (event ID, data) pairs from an SSE response."""
    data_lines: list[str] = []
    event_id: str | None = None
    last_event_id: str | None = None

    for raw_line in response.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue
        line = raw_line.lstrip("\ufeff")
        if line == "":
            if data_lines:
                yield event_id or last_event_id, "\n".join(data_lines)
            data_lines = []
            event_id = None
            continue
        if line.startswith(":"):
            continue
        field, separator, value = line.partition(":")
        if separator and value.startswith(" "):
            value = value[1:]
        if field == "data":
            data_lines.append(value)
        elif field == "id" and "\x00" not in value:
            event_id = value
            last_event_id = value

    if data_lines:
        yield event_id or last_event_id, "\n".join(data_lines)


def stream_forever(
    producer,
    topic: str,
    db: sqlite3.Connection,
    session: requests.Session,
) -> None:
    last_event_id = load_last_event_id(db)
    retry_seconds = 5.0
    published_count = 0
    while True:
        headers = {
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
            "User-Agent": os.getenv(
                "WIKIMEDIA_USER_AGENT",
                "BigDataClusterLiveEdits/1.0 (educational real-time analytics project)",
            ),
        }
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id

        heartbeat_stop = None
        heartbeat_thread = None
        disconnected = False
        disconnect_detail = "Wikimedia closed the SSE connection"
        pending_checkpoint_id = None
        events_since_checkpoint = 0
        try:
            LOGGER.info("Connecting to Wikimedia RecentChange EventStreams")
            with session.get(
                STREAM_URL,
                headers=headers,
                stream=True,
                timeout=(15, 90),
            ) as response:
                response.raise_for_status()
                response.encoding = "utf-8"
                send_status(producer, topic, "connected")
                heartbeat_stop, heartbeat_thread = start_heartbeat(producer, topic)
                retry_seconds = 5.0
                LOGGER.info("Connected to Wikimedia's continuous SSE stream")
                for sse_id, raw_data in iter_sse_messages(response):
                    try:
                        data = json.loads(raw_data)
                        if not isinstance(data, dict):
                            raise ValueError("RecentChange event must be a JSON object")
                        event = normalize_recent_change(data, sse_id)
                        publish(producer, topic, event["event_id"], event)
                        if sse_id:
                            last_event_id = sse_id
                            pending_checkpoint_id = sse_id
                            events_since_checkpoint += 1
                            if events_since_checkpoint >= 100:
                                save_last_event_id(db, pending_checkpoint_id)
                                pending_checkpoint_id = None
                                events_since_checkpoint = 0
                        published_count += 1
                        if published_count == 1 or published_count % 100 == 0:
                            LOGGER.info(
                                "Published %s Wikimedia events; latest event ID=%s",
                                published_count,
                                event["event_id"],
                            )
                    except (json.JSONDecodeError, ValueError, TypeError):
                        LOGGER.exception("Skipping malformed Wikimedia stream event")
                disconnected = True
        except KeyboardInterrupt:
            raise
        except (requests.RequestException, OSError, RuntimeError) as error:
            disconnected = True
            disconnect_detail = str(error)
        finally:
            if heartbeat_stop:
                heartbeat_stop.set()
            if heartbeat_thread:
                heartbeat_thread.join(timeout=2)
            if pending_checkpoint_id:
                save_last_event_id(db, pending_checkpoint_id)
        if disconnected:
            LOGGER.warning(
                "Wikimedia stream disconnected: %s; reconnecting in %.1f seconds",
                disconnect_detail,
                retry_seconds,
            )
            try:
                send_status(producer, topic, "reconnecting", disconnect_detail)
            except Exception:
                LOGGER.exception("Could not publish stream-disconnect status")
            time.sleep(retry_seconds + random.uniform(0, retry_seconds * 0.2))
            retry_seconds = min(retry_seconds * 2, 60.0)


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
        from kafka import KafkaProducer
    except ImportError as error:
        raise RuntimeError(
            "Kafka publishing requires kafka-python; install requirements.txt"
        ) from error

    producer = KafkaProducer(
        bootstrap_servers=[
            server.strip() for server in args.bootstrap_servers.split(",")
        ],
        acks="all",
        retries=5,
        linger_ms=10,
    )
    session = requests.Session()
    db = init_resume_store(args.db)
    LOGGER.info(
        "Publishing Wikimedia's continuous RecentChange SSE stream to %s",
        args.topic.strip(),
    )
    try:
        stream_forever(producer, args.topic.strip(), db, session)
    except KeyboardInterrupt:
        LOGGER.info("Stopping Wikimedia stream producer")
        try:
            send_status(producer, args.topic.strip(), "stopped")
        except Exception:
            LOGGER.exception("Could not publish final stream status")
    finally:
        db.close()
        session.close()
        producer.close(timeout=30)


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    main()
