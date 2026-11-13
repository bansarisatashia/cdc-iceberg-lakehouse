"""Integration test: apply CDC batches to real Iceberg tables in a local SQLite-backed catalog."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

pytest.importorskip("sqlalchemy")
from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402

from cdc_events import latest_per_key, parse  # noqa: E402
from iceberg_sink import apply_batch, ensure_tables  # noqa: E402


def ev(op, lsn, before=None, after=None):
    return parse(json.dumps({"op": op, "before": before, "after": after, "ts_ms": lsn,
                             "source": {"table": "orders", "lsn": lsn}}))


def order(i, status, amount=20.0):
    return {"order_id": i, "customer_id": 1, "status": status, "amount": amount,
            "created_at": 1_700_000_000_000, "updated_at": 1_700_000_000_000}


@pytest.fixture
def tables(tmp_path):
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")
    return ensure_tables(catalog)


def test_upserts_and_deletes(tables):
    apply_batch(tables, latest_per_key([ev("c", 1, after=order(1, "PLACED")), ev("c", 2, after=order(2, "PLACED")),
                                        ev("c", 3, after=order(3, "PLACED"))]))
    apply_batch(tables, latest_per_key([ev("u", 4, after=order(1, "SHIPPED")),
                                        ev("d", 5, before=order(2, "CANCELLED"))]))
    df = tables["orders"].scan().to_arrow().to_pandas().set_index("order_id")
    assert sorted(df.index) == [1, 3]
    assert df.loc[1, "status"] == "SHIPPED"
    assert df.loc[1, "_cdc_op"] == "u"


def test_replaying_a_batch_is_idempotent(tables):
    batch = latest_per_key([ev("c", 1, after=order(1, "PLACED")), ev("u", 2, after=order(1, "PAID"))])
    apply_batch(tables, batch)
    apply_batch(tables, batch)
    assert tables["orders"].scan().to_arrow().num_rows == 1
