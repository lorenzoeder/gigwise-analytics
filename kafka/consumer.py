"""Kafka consumer: streams event updates from Kafka to BigQuery.

Reads from the ``ticket_events`` topic and inserts rows into
``streaming.live_event_updates`` in BigQuery using the streaming insert API.

Toggle on/off with ``make run-streaming`` / ``make stop-streaming``.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from pathlib import Path

from kafka import KafkaConsumer
from pathlib import Path

# BigQuery table schema for auto-creation
_TABLE_SCHEMA = [
    {"name": "event_id", "field_type": "STRING", "mode": "REQUIRED"},
    {"name": "event_name", "field_type": "STRING", "mode": "NULLABLE"},
    {"name": "artist_name", "field_type": "STRING", "mode": "NULLABLE"},
    {"name": "event_date", "field_type": "STRING", "mode": "NULLABLE"},
    {"name": "event_status", "field_type": "STRING", "mode": "NULLABLE"},
    {"name": "venue_name", "field_type": "STRING", "mode": "NULLABLE"},
    {"name": "city", "field_type": "STRING", "mode": "NULLABLE"},
    {"name": "country_code", "field_type": "STRING", "mode": "NULLABLE"},
    {"name": "observed_at", "field_type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "is_new", "field_type": "BOOLEAN", "mode": "NULLABLE"},
]


def _ensure_table(client, table_ref: str) -> None:
    """Create or recreate the streaming table with the correct schema."""
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound

    expected_fields = {f["name"] for f in _TABLE_SCHEMA}
    try:
        existing = client.get_table(table_ref)
        actual_fields = {f.name for f in existing.schema}
        if not expected_fields.issubset(actual_fields):
            missing = expected_fields - actual_fields
            print(f"[consumer] Schema mismatch (missing: {missing}) — recreating table", flush=True)
            client.delete_table(table_ref)
            raise NotFound("recreate")
        return
    except NotFound:
        schema = [bigquery.SchemaField(**f) for f in _TABLE_SCHEMA]
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="observed_at")
        client.create_table(table)
        print(f"[consumer] Created table {table_ref}", flush=True)


def main() -> None:
    project_id = os.getenv("GCP_PROJECT_ID", "").strip()
    if not project_id:
        print("[consumer] GCP_PROJECT_ID not set — exiting", file=sys.stderr)
        raise SystemExit(1)

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "ticket_events")
    dataset = os.getenv("BQ_DATASET_STREAMING", "streaming")
    table_name = "live_event_updates"
    flush_interval = int(os.getenv("CONSUMER_FLUSH_INTERVAL", "10"))
    # Write PID file for clean shutdown via make stop-streaming
    pid_path = Path(__file__).resolve().parent.parent / "logs" / "consumer.pid"
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(os.getpid()))
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.{table_name}"
    _ensure_table(client, table_ref)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="gigwise-bq-consumer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=flush_interval * 1000,
    )

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    print(f"[consumer] Consuming from {topic} → {table_ref}", flush=True)
    buffer: list[dict] = []

    while running:
        try:
            for message in consumer:
                if not running:
                    break
                buffer.append(message.value)

                if len(buffer) >= 50:
                    _flush(client, table_ref, buffer)
                    buffer = []
        except Exception:
            pass  # consumer_timeout_ms triggers StopIteration

        if buffer:
            _flush(client, table_ref, buffer)
            buffer = []

    consumer.close()
    pid_path.unlink(missing_ok=True)
    print("[consumer] Shut down cleanly", flush=True)


def _flush(client, table_ref: str, rows: list[dict]) -> None:
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        print(f"[consumer] BigQuery insert errors: {errors}", file=sys.stderr, flush=True)
    else:
        print(f"[consumer] Flushed {len(rows)} rows to BigQuery", flush=True)


if __name__ == "__main__":
    main()
