import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cdc_events import latest_per_key, parse  # noqa: E402


def envelope(op, before=None, after=None, lsn=1, table="orders", ts=1000):
    return json.dumps(
        {"op": op, "before": before, "after": after, "ts_ms": ts, "source": {"table": table, "lsn": lsn}}
    )


def test_parse_insert():
    ev = parse(envelope("c", after={"order_id": 7, "status": "PLACED", "amount": 10.0}))
    assert ev.op == "c" and ev.key == 7 and ev.row["status"] == "PLACED"


def test_parse_delete_uses_before_image():
    ev = parse(envelope("d", before={"order_id": 7, "status": "CANCELLED"}))
    assert ev.is_delete and ev.key == 7 and ev.row is None


def test_parse_ignores_tombstones_and_unknown_tables():
    assert parse(None) is None
    assert parse("null") is None
    assert parse(envelope("c", after={"id": 1}, table="audit_log")) is None


def test_latest_per_key_keeps_highest_lsn():
    events = [
        parse(envelope("c", after={"order_id": 1, "status": "PLACED"}, lsn=10)),
        parse(envelope("u", after={"order_id": 1, "status": "SHIPPED"}, lsn=30)),
        parse(envelope("u", after={"order_id": 1, "status": "PAID"}, lsn=20)),  # arrives late
        parse(envelope("c", after={"order_id": 2, "status": "PLACED"}, lsn=15)),
    ]
    latest = latest_per_key(events)["orders"]
    assert latest[1].row["status"] == "SHIPPED"
    assert set(latest) == {1, 2}


def test_insert_then_delete_collapses_to_delete():
    events = [
        parse(envelope("c", after={"order_id": 5, "status": "PLACED"}, lsn=1)),
        parse(envelope("d", before={"order_id": 5, "status": "PLACED"}, lsn=2)),
    ]
    assert latest_per_key(events)["orders"][5].is_delete
