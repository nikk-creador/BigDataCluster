# Wikimedia Live Changes Big Data Dashboard

This project consumes Wikimedia's **continuous RecentChange EventStreams** feed
using Server-Sent Events (SSE), publishes events to Kafka, processes the stream
with a PySpark standalone cluster, and presents live changes and the 8 Vs in a
Streamlit dashboard.

The source is a persistent event stream—not periodic polling. New events arrive
as Wikimedia emits them. The pipeline reconnects with `Last-Event-ID` when the
connection drops, and SQLite deduplicates replayed events. The stored event
schema intentionally excludes usernames and edit comments.

## Architecture

```text
Wikimedia EventStreams (SSE)
  -> live_producer.py
  -> Kafka topic: wikimedia.recentchange.v1
       -> stream_consumer.py -> wikimedia_live.db -> live feed / trends
       -> Spark Structured Streaming -> spark_data/wikimedia/*.json
  -> Streamlit dashboard
```

Wikimedia's [EventStreams documentation](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams)
describes the SSE service, available streams, schemas, and reconnection support.
The [RecentChange stream](https://stream.wikimedia.org/v2/stream/recentchange)
contains public change events from Wikimedia projects.

## Prerequisites

- Docker Desktop with Compose.
- Python 3.10+ and the project dependencies.
- For remote Spark workers: Java 17, Spark 4.3.0, compatible Python, and a
  reachable private network.

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

The producer uses Wikimedia's public stream; no API key is required. A
descriptive `WIKIMEDIA_USER_AGENT` may be set in `.env`.
The local Python producer, consumer, and dashboard load settings from this
project's `.env`, overriding stale PowerShell environment values. They log
the selected Kafka bootstrap address when started; use `--bootstrap-servers`
to explicitly select a different broker for either Kafka process.

## Start the live pipeline

Run PowerShell from the project directory.

### 1. Start Kafka and the local Spark cluster

```powershell
docker compose up -d kafka spark-master spark-worker
docker compose ps
```

Wait for Kafka, `spark-master`, and `spark-worker` to become healthy. The Spark
master UI is at http://localhost:8080.

### 2. Create the Kafka topic

```powershell
docker exec wikimedia-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9095 --create --if-not-exists --topic wikimedia.recentchange.v1 --partitions 3 --replication-factor 1
```

Verify the topic:

```powershell
docker exec wikimedia-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9095 --describe --topic wikimedia.recentchange.v1
```

### 3. Start Spark analytics

```powershell
docker compose up -d spark-analytics
docker compose logs -f spark-analytics
```

The Spark service connects to the Kafka topic, analyzes micro-batches on the
cluster, and writes its latest report under `spark_data/wikimedia/`. The first
start downloads the Spark Kafka connector.

### 4. Start the producer, consumer, and dashboard

Open three PowerShell terminals in the project directory. Keep each process
running:

Terminal 1 — consume Kafka into SQLite:

```powershell
python stream_consumer.py
```

Terminal 2 — connect to Wikimedia SSE and publish events:

```powershell
python live_producer.py
```

The producer sends RecentChange events as they arrive, emits Kafka heartbeats
while connected, retries temporary failures with backoff, and resumes from its
stored stream event ID after reconnecting. To see the connection and publish
logs, leave this terminal visible.

Terminal 3 — dashboard:

```powershell
streamlit run app.py
```

Open the Streamlit URL printed in the terminal (normally
http://localhost:8501). The dashboard refreshes every five seconds.

## Dashboard and the 8 Vs

### What data is processed?

The project consumes Wikimedia RecentChange **event metadata**, not full
article text. Retained fields cover event identity and timestamps; wiki and page
context; change type and bot/minor flags; revision IDs; old/new page lengths and
their delta; plus message size for volume analysis. Usernames and edit comments
are intentionally omitted. Change types include events such as edits, new
pages, logs, and categorization. Namespace IDs identify page areas such as
articles, files, templates, and categories.

### How is it processed?

1. `live_producer.py` reads the continuous Wikimedia SSE stream, normalizes
   events, and omits editor details.
2. The producer publishes keyed JSON events to Kafka topic
   `wikimedia.recentchange.v1`.
3. `stream_consumer.py` validates and deduplicates event IDs before storing
   events in SQLite. Spark Structured Streaming independently analyzes Kafka
   micro-batches and writes analytics reports.
4. The dashboard reads SQLite for its event feed and metrics; it refreshes
   every five seconds. Spark reports are displayed separately.

**Variety** in this project means the mixture of event classifications,
Wikimedia projects, page namespaces, revision measurements, and pipeline
quality/status data—not different formats of full article content.

- **Volume:** Events in the selected window, retained payload size, and
  cumulative events consumed.
- **Velocity:** Live events per minute and a stacked arrivals-per-minute chart.
- **Variety:** Wiki, change-type, and namespace distributions.
- **Veracity:** Required-field completeness, duplicate IDs ignored, and invalid
  Kafka messages recorded.
- **Value:** Page bytes added/removed, net size change, and an event-impact
  breakdown.
- **Variability:** Five-minute arrival-rate chart with peak, average, and
  standard-deviation metrics.
- **Validity:** Percent of records with valid IDs, wiki, title, timestamp, page
  URL, and payload.
- **Volatility:** Age of the oldest/newest event in view and daily retained
  event volume.

The sidebar filters both the live event table and analytics by wiki and time
window. Velocity and variability use the pipeline's `received_at` timestamp
(arrival time), while content analytics use Wikimedia's `occurred_at` event
timestamp. This keeps live-rate charts accurate when source events arrive late.
Charts have hover details and zoom where useful; the dashboard refreshes every
five seconds.

The Live edit feed shows page, project, change type, namespace, event time, and
page-length change. The dashboard does not display or persist editor usernames
or edit comments. Event history is retained locally for up to seven days.

## Add a remote Spark worker

First find the master PC's active Wi-Fi/Ethernet IPv4 address with `ipconfig`.
Use an address reachable from the worker on the same trusted private network;
do not use `localhost` or a Docker-only address. This workspace's `.env` uses
the current Wi-Fi address `10.100.149.90`; replace it if `ipconfig` shows a
different reachable address. The host-side Kafka client bootstraps through
`localhost:9092`; Kafka then advertises the LAN address to remote clients.

Set the master address in `.env`:

```env
KAFKA_ADVERTISED_HOST=MASTER_IP
KAFKA_BOOTSTRAP_SERVERS=MASTER_IP:9092
KAFKA_SPARK_ADVERTISED_HOST=MASTER_IP
SPARK_KAFKA_BOOTSTRAP_SERVERS=MASTER_IP:9094
SPARK_CLUSTER_PUBLIC_HOST=MASTER_IP
```

Recreate the services:

```powershell
docker compose up -d --force-recreate kafka spark-master spark-worker spark-analytics
```

On the worker PC, test Spark master connectivity:

```powershell
Test-NetConnection MASTER_IP -Port 7077
```

Configure Java 17, Spark 4.3.0, and a compatible Python version on the remote
worker. Set `JAVA_HOME`, `SPARK_HOME`, and `PYSPARK_PYTHON`. Allow these TCP
ports through Windows Firewall on the **Private** network only:

- Master PC: `7077`, `7080-7081`, `9092`, and `9094`.
- Remote worker: `7078`, `7082-7083`; `8081` is optional for the worker UI.
- Keep Kafka controller port `9093` private.

Start the remote worker (replace both addresses):

```powershell
$env:SPARK_LOCAL_IP = "WORKER_IP"
& "$env:SPARK_HOME\bin\spark-class.cmd" org.apache.spark.deploy.worker.Worker --host WORKER_IP --port 7078 --webui-port 8081 --cores 2 --memory 2g spark://MASTER_IP:7077
```

Leave the process running. The worker should show as **ALIVE** at
http://MASTER_IP:8080. Registration does not guarantee that tasks can run:
check worker logs for runtime errors. A prior Windows worker reported missing
`HADOOP_HOME`; configure the required Windows Hadoop utilities or use a
Linux/WSL/Docker worker for a more consistent Spark runtime.

Spark standalone security is not enabled by default. Keep the cluster on a
trusted private LAN; do not expose its ports to the public internet.

## Troubleshooting

| Symptom | Check |
|---|---|
| Stream status is waiting or reconnecting | Inspect `live_producer.py` logs and verify internet access to `stream.wikimedia.org`. |
| No events appear in the dashboard | Confirm producer and consumer are running, the topic names match, and Kafka is healthy with `docker compose ps`. |
| Spark report is missing | Inspect `docker compose logs -f spark-analytics`, confirm the topic exists, and verify workers are alive in the Spark master UI. |
| Worker cannot connect | Run `Test-NetConnection MASTER_IP -Port 7077`; verify LAN IP, firewall, and Docker port publishing. |
| Worker is alive but Spark loses executors | Inspect its logs for Java/Python/Hadoop prerequisites; registration alone is insufficient. |

## Stop the project

Press Ctrl+C in each Python terminal. Stop following Docker logs with Ctrl+C,
then stop the Compose services:

```powershell
docker compose down
```

This preserves the Kafka data volume and local database/report files. Avoid
`docker compose down -v` unless you intentionally want to delete Kafka's
persisted data.
