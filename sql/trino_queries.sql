-- Run in Trino: docker compose exec trino trino --catalog iceberg --schema shop

-- Current state of orders (row-level updates/deletes already applied)
SELECT status, count(*) AS orders, round(sum(amount), 2) AS revenue
FROM orders GROUP BY status ORDER BY orders DESC;

-- Revenue by customer state
SELECT c.state, count(*) AS orders, round(sum(o.amount), 2) AS revenue
FROM orders o JOIN customers c USING (customer_id)
GROUP BY c.state ORDER BY revenue DESC;

-- Iceberg snapshot history: every sink commit is a snapshot
SELECT committed_at, snapshot_id, operation, summary['added-records'] AS added
FROM "orders$snapshots" ORDER BY committed_at DESC LIMIT 10;

-- Time travel: the table as of an earlier snapshot
-- SELECT count(*) FROM orders FOR VERSION AS OF <snapshot_id>;
