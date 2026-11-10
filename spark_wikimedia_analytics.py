"""Analyze Wikimedia RecentChange Kafka events with Spark Structured Streaming."""
import json
import logging
import os
import socket
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

LOGGER = logging.getLogger("wikimedia-spark-analytics")

EVENT_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("event_id", StringType()),
        StructField("occurred_at", StringType()),
        StructField("received_at", StringType()),
        StructField("stream", StringType()),
        StructField("status", StringType()),
        StructField("wiki", StringType()),
        StructField("server_name", StringType()),
        StructField("title", StringType()),
        StructField("page_url", StringType()),
        StructField("change_type", StringType()),
        StructField("namespace", IntegerType()),
        StructField("bot", BooleanType()),
        StructField("minor", BooleanType()),
        StructField("length_old", LongType()),
        StructField("length_new", LongType()),
        StructField("length_delta", LongType()),
        StructField("revision_old", LongType()),
        StructField("revision_new", LongType()),
    ]
)


def atomic_json_write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(payload, output, ensure_ascii=False, separators=(",", ":"))
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def load_json(path: Path, fallback: dict) -> dict:
    try:
        with path.open(encoding="utf-8") as source:
            payload = json.load(source)
        return payload if isinstance(payload, dict) else fallback
    except (OSError, json.JSONDecodeError):
        return fallback


def count_by(frame, column: str) -> dict:
    return {
        str(row[column] if row[column] is not None else "unknown"): int(row["count"])
        for row in frame.groupBy(column).count().collect()
    }


def worker_hosts_for_partition(rows):
    if any(True for _ in rows):
        yield socket.gethostname()


def main() -> None:
    topic = os.getenv("WIKIMEDIA_KAFKA_TOPIC", "wikimedia.recentchange.v1")
    bootstrap_servers = os.getenv("SPARK_KAFKA_BOOTSTRAP_SERVERS", "kafka:9095")
    output_dir = Path(
        os.getenv("SPARK_ANALYTICS_OUTPUT_DIR", "/opt/spark-data/wikimedia")
    )
    checkpoint_dir = Path(
        os.getenv(
            "SPARK_ANALYTICS_CHECKPOINT_DIR",
            str(output_dir / "checkpoint"),
        )
    )
    report_path = output_dir / "latest_analytics.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    previous_report = load_json(report_path, {})
    cumulative_events = int(previous_report.get("total_kafka_records", 0))

    spark = (
        SparkSession.builder.appName("Wikimedia-RecentChange-Live-Analytics")
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    LOGGER.info(
        "Starting Wikimedia Structured Streaming: topic=%s bootstrap=%s master=%s",
        topic,
        bootstrap_servers,
        spark.sparkContext.master,
    )

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", os.getenv("SPARK_KAFKA_STARTING_OFFSETS", "earliest"))
        .option("maxOffsetsPerTrigger", os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "10000"))
        .option("failOnDataLoss", "false")
        .load()
    )
    events = (
        source.selectExpr(
            "CAST(value AS STRING) AS value_text",
            "timestamp AS kafka_timestamp",
        )
        .withColumn("event", F.from_json(F.col("value_text"), EVENT_SCHEMA))
        .select(
            "value_text",
            "kafka_timestamp",
            "event.*",
        )
    )

    def process_batch(batch, batch_id: int) -> None:
        nonlocal cumulative_events
        started = time.monotonic()
        batch.persist()
        try:
            record_count = batch.count()
            if record_count == 0:
                return

            recent = batch.filter(F.col("event_type") == "recent_change")
            recent_count = recent.count()
            malformed_count = batch.filter(
                F.col("event_type").isNull()
                | ~F.col("event_type").isin("recent_change", "stream_status")
            ).count()
            duplicate_ids = (
                recent_count - recent.select("event_id").distinct().count()
                if recent_count
                else 0
            )
            byte_count = (
                batch.agg(
                    F.sum(F.length(F.encode(F.col("value_text"), "UTF-8"))).alias(
                        "bytes"
                    )
                ).first()["bytes"]
                or 0
            )
            type_counts = count_by(recent, "change_type") if recent_count else {}
            wiki_counts = count_by(recent, "wiki") if recent_count else {}
            namespace_counts = (
                count_by(recent, "namespace") if recent_count else {}
            )
            status_counts = count_by(
                batch.filter(F.col("event_type") == "stream_status"),
                "status",
            )

            if recent_count:
                quality_row = recent.agg(
                    F.sum(F.when(F.col("wiki").isNull() | (F.col("wiki") == ""), 1).otherwise(0)).alias("missing_wiki"),
                    F.sum(F.when(F.col("title").isNull() | (F.col("title") == ""), 1).otherwise(0)).alias("missing_title"),
                    F.sum(F.when(F.col("occurred_at").isNull() | (F.col("occurred_at") == ""), 1).otherwise(0)).alias("missing_timestamp"),
                    F.sum(F.when(F.col("event_id").isNull() | (F.col("event_id") == ""), 1).otherwise(0)).alias("missing_event_id"),
                    F.sum(F.when(F.col("bot"), 1).otherwise(0)).alias("bot_events"),
                    F.sum(F.when(F.col("minor"), 1).otherwise(0)).alias("minor_events"),
                    F.avg("length_delta").alias("average_length_delta"),
                ).first().asDict()
            else:
                quality_row = {}

            worker_hosts = sorted(
                set(
                    batch.select("value_text")
                    .rdd.mapPartitions(worker_hosts_for_partition)
                    .collect()
                )
            )
            elapsed_seconds = max(time.monotonic() - started, 0.001)
            cumulative_events += record_count
            report = {
                "application": "Wikimedia-RecentChange-Live-Analytics",
                "spark_application_id": spark.sparkContext.applicationId,
                "spark_version": spark.version,
                "spark_master": spark.sparkContext.master,
                "worker_hosts_used": worker_hosts,
                "topic": topic,
                "batch_id": int(batch_id),
                "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "batch_kafka_records": int(record_count),
                "batch_recent_changes": int(recent_count),
                "total_kafka_records": int(cumulative_events),
                "batch_payload_bytes": int(byte_count),
                "batch_events_per_second": round(record_count / elapsed_seconds, 2),
                "recent_changes_per_second": round(recent_count / elapsed_seconds, 2),
                "change_type_counts": type_counts,
                "wiki_counts": wiki_counts,
                "namespace_counts": namespace_counts,
                "stream_status_counts": status_counts,
                "data_quality": {
                    "malformed_or_unknown_events_in_batch": int(malformed_count),
                    "duplicate_ids_in_batch": int(duplicate_ids),
                    "missing_wiki": int(quality_row.get("missing_wiki") or 0),
                    "missing_title": int(quality_row.get("missing_title") or 0),
                    "missing_timestamp": int(
                        quality_row.get("missing_timestamp") or 0
                    ),
                    "missing_event_id": int(
                        quality_row.get("missing_event_id") or 0
                    ),
                    "bot_events": int(quality_row.get("bot_events") or 0),
                    "minor_events": int(quality_row.get("minor_events") or 0),
                    "average_page_length_delta": (
                        float(quality_row["average_length_delta"])
                        if quality_row.get("average_length_delta") is not None
                        else None
                    ),
                },
            }
            atomic_json_write(report_path, report)
            LOGGER.info(
                "Spark batch %s: kafka_records=%s recent_changes=%s workers=%s",
                batch_id,
                record_count,
                recent_count,
                ",".join(worker_hosts) or "none",
            )
        finally:
            batch.unpersist()

    query = (
        events.writeStream.queryName("wikimedia-recentchange-analytics")
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(processingTime=os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds"))
        .foreachBatch(process_batch)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("SPARK_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    main()
