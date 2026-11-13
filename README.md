# CDC Lakehouse: Postgres → Debezium → Kafka → Apache Iceberg → Trino

A change-data-capture pipeline that keeps an Apache Iceberg lakehouse in sync with an operational Postgres database in near real time. Debezium streams every insert, update and delete from the Postgres WAL into Kafka. A Python sink applies them to Iceberg tables on S3-compatible storage (MinIO) with row-level upserts and deletes. Trino queries the result, including time travel over snapshots.

> Personal portfolio project. The "shop" database and its traffic are simulated.

## Architecture

```
┌────────────┐  WAL   ┌───────────┐       ┌───────┐       ┌────────────────────┐      ┌──────────────────┐
│ Postgres 16│ ─────→ │ Debezium  │ ────→ │ Kafka │ ────→ │ Python Iceberg sink│ ───→ │ Iceberg tables    │
│ customers  │pgoutput│ (Kafka    │ topic │       │ micro │ • latest-per-key   │      │ on MinIO (S3)     │
│ orders     │        │  Connect) │ /table│       │ batch │ • upsert + delete  │      │ REST catalog      │
└────────────┘        └───────────┘       └───────┘       │ • commit offsets   │      └────────┬─────────┘
      ↑                                                   │   after Iceberg    │               ↓
 simulate_oltp.py                                         └────────────────────┘        Trino (SQL, time travel)
```

## How the sink guarantees correctness

1. **Ordering:** each micro-batch is collapsed to the latest change per primary key, using the Postgres LSN. Late or out-of-order events can't overwrite newer state.
2. **Upserts and deletes:** creates and updates go through PyIceberg `upsert` on the table's identifier field. Deletes become Iceberg row-level deletes.
3. **Exactly-once table state:** Kafka offsets are committed only after the Iceberg commit succeeds. If the sink crashes, the batch is replayed, and because upserts are idempotent the table ends up the same (covered by a test).
4. **Lineage columns:** every row carries `_cdc_op`, `_cdc_lsn` and `_ingested_at`.

## Run it

Requirements: Docker and Docker Compose.

```bash
docker compose up -d --build
./scripts/register_connector.sh                       # start Debezium capture
pip install -r requirements.txt
python scripts/simulate_oltp.py --ops-per-sec 20      # inserts, status updates, deletes
```

Query the lakehouse:

```bash
docker compose exec trino trino --catalog iceberg --schema shop
```

`sql/trino_queries.sql` has example queries: order status mix, revenue by state, snapshot history and time travel.

UIs: MinIO console at http://localhost:9001 (admin / password), Trino at http://localhost:8080.

## Tests

```bash
pytest -q
```

- `test_cdc_events.py`: Debezium envelope parsing, tombstones, and LSN ordering with late events.
- `test_iceberg_sink.py`: integration tests against real Iceberg tables in a local SQLite-backed catalog. They check that upserts, deletes and replays leave the table correct.

## Project structure

```
src/cdc_events.py            Debezium envelope parsing + latest-per-key compaction
src/iceberg_sink.py          Kafka consumer → PyIceberg upserts/deletes
scripts/simulate_oltp.py     OLTP traffic generator
scripts/register_connector.sh
connectors/postgres-source.json   Debezium connector config
sql/init.sql                 source schema + publication
sql/trino_queries.sql        analytics + time-travel queries
trino/catalog/iceberg.properties
docker-compose.yml           Postgres, Kafka, Debezium, MinIO, Iceberg REST, sink, Trino
```

## Next steps

- Schedule Iceberg maintenance (compaction, snapshot expiry, orphan-file cleanup).
- Switch the sink to the Apache Iceberg Kafka Connect sink for higher throughput, and compare.
- Deploy on AWS: MSK, Glue Data Catalog and S3, provisioned with Terraform.

## Tech

Python · PostgreSQL · Debezium · Apache Kafka · Apache Iceberg (PyIceberg) · MinIO (S3) · Trino · Docker
