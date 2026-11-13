"""Parse Debezium change events and collapse a micro-batch to the latest state per key.

Debezium envelope (JSON converter, schemas disabled):
    {"before": {...} | null, "after": {...} | null, "op": "c|u|d|r",
     "source": {"lsn": 123, "ts_ms": ..., "table": "orders", ...}, "ts_ms": ...}
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

PRIMARY_KEYS = {"customers": "customer_id", "orders": "order_id"}


@dataclass(frozen=True)
class ChangeEvent:
    table: str
    op: str            # c (create), u (update), d (delete), r (snapshot read)
    key: int
    row: dict | None   # state after the change; None for deletes
    lsn: int           # Postgres log sequence number, gives a total order
    ts_ms: int

    @property
    def is_delete(self) -> bool:
        return self.op == "d"


def parse(raw: bytes | str) -> ChangeEvent | None:
    """Turn one Kafka message value into a ChangeEvent. Returns None for tombstones/heartbeats."""
    if raw is None:
        return None
    payload = json.loads(raw)
    if payload is None or "op" not in payload:
        return None
    source = payload.get("source", {})
    table = source.get("table")
    if table not in PRIMARY_KEYS:
        return None
    pk = PRIMARY_KEYS[table]
    image = payload.get("after") if payload["op"] != "d" else payload.get("before")
    if not image or pk not in image:
        return None
    return ChangeEvent(
        table=table,
        op=payload["op"],
        key=int(image[pk]),
        row=None if payload["op"] == "d" else payload["after"],
        lsn=int(source.get("lsn") or 0),
        ts_ms=int(payload.get("ts_ms") or source.get("ts_ms") or 0),
    )


def latest_per_key(events: Iterable[ChangeEvent]) -> dict[str, dict[int, ChangeEvent]]:
    """Keep only the last change for every (table, key), ordered by LSN then event time.

    This makes the sink idempotent per batch: replaying the same Kafka offsets
    produces the same upserts and deletes.
    """
    out: dict[str, dict[int, ChangeEvent]] = {}
    for ev in events:
        bucket = out.setdefault(ev.table, {})
        cur = bucket.get(ev.key)
        if cur is None or (ev.lsn, ev.ts_ms) >= (cur.lsn, cur.ts_ms):
            bucket[ev.key] = ev
    return out
