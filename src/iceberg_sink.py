"""Kafka (Debezium) -> Apache Iceberg sink using PyIceberg.

Reads change events in micro-batches, collapses them to the latest state per
primary key, then applies them to Iceberg tables:
  * creates / updates  -> MERGE-style upsert on the primary key
  * deletes            -> row-level delete
Offsets are committed only after the Iceberg commit succeeds (at-least-once
delivery + idempotent upserts = effectively exactly-once table state).
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import In

from cdc_events import ChangeEvent, latest_per_key, parse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("iceberg-sink")

NAMESPACE = "shop"
SCHEMAS = {
    "customers": pa.schema(
        [
            pa.field("customer_id", pa.int64(), nullable=False),
            pa.field("email", pa.string()),
            pa.field("full_name", pa.string()),
            pa.field("state", pa.string()),
            pa.field("created_at", pa.timestamp("us", tz="UTC")),
            pa.field("updated_at", pa.timestamp("us", tz="UTC")),
            pa.field("_cdc_op", pa.string()),
            pa.field("_cdc_lsn", pa.int64()),
            pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
    "orders": pa.schema(
        [
            pa.field("order_id", pa.int64(), nullable=False),
            pa.field("customer_id", pa.int64()),
            pa.field("status", pa.string()),
            pa.field("amount", pa.float64()),
            pa.field("created_at", pa.timestamp("us", tz="UTC")),
            pa.field("updated_at", pa.timestamp("us", tz="UTC")),
            pa.field("_cdc_op", pa.string()),
            pa.field("_cdc_lsn", pa.int64()),
            pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
}
PK = {"customers": "customer_id", "orders": "order_id"}


def _ts(value):
    """Debezium 'connect' time precision sends epoch millis."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def to_arrow(table: str, events: list[ChangeEvent]) -> pa.Table:
    schema = SCHEMAS[table]
    now = datetime.now(timezone.utc)
    rows = []
    for ev in events:
        r = dict(ev.row or {})
        for col in ("created_at", "updated_at"):
            r[col] = _ts(r.get(col))
        r["_cdc_op"] = ev.op
        r["_cdc_lsn"] = ev.lsn
        r["_ingested_at"] = now
        rows.append({f.name: r.get(f.name) for f in schema})
    return pa.Table.from_pylist(rows, schema=schema)


def ensure_tables(catalog):
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    tables = {}
    for name, schema in SCHEMAS.items():
        ident = f"{NAMESPACE}.{name}"
        try:
            tables[name] = catalog.load_table(ident)
        except NoSuchTableError:
            log.info("creating Iceberg table %s", ident)
            tables[name] = catalog.create_table(ident, schema=schema)
            with tables[name].update_schema() as upd:
                upd.set_identifier_fields(PK[name])
    return tables


def apply_batch(tables, batch: dict[str, dict[int, ChangeEvent]]) -> dict[str, tuple[int, int]]:
    stats = {}
    for table, by_key in batch.items():
        tbl = tables[table]
        upserts = [e for e in by_key.values() if not e.is_delete]
        deletes = [e.key for e in by_key.values() if e.is_delete]
        if upserts:
            tbl.upsert(to_arrow(table, upserts), join_cols=[PK[table]])
        if deletes:
            tbl.delete(delete_filter=In(PK[table], deletes))
        stats[table] = (len(upserts), len(deletes))
    return stats


def main() -> None:
    from confluent_kafka import Consumer

    catalog = load_catalog(
        "rest",
        **{
            "uri": os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181"),
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
            "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )
    tables = ensure_tables(catalog)

    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
            "group.id": "iceberg-sink",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["shop.public.customers", "shop.public.orders"])
    batch_size = int(os.getenv("BATCH_SIZE", "5000"))
    flush_seconds = float(os.getenv("FLUSH_SECONDS", "15"))

    buffer: list[ChangeEvent] = []
    last_flush = time.monotonic()
    while True:
        for msg in consumer.consume(num_messages=1000, timeout=1.0):
            if msg.error():
                log.warning("kafka error: %s", msg.error())
                continue
            ev = parse(msg.value())
            if ev:
                buffer.append(ev)
        if buffer and (len(buffer) >= batch_size or time.monotonic() - last_flush >= flush_seconds):
            stats = apply_batch(tables, latest_per_key(buffer))
            consumer.commit(asynchronous=False)
            log.info("committed %d events -> %s", len(buffer), stats)
            buffer.clear()
            last_flush = time.monotonic()


if __name__ == "__main__":
    main()
