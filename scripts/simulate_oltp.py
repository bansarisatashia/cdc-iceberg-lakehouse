"""Simulate OLTP traffic: new customers, new orders, status updates and some deletes."""
from __future__ import annotations

import argparse
import random
import time

import psycopg

STATES = ["TX", "CA", "NY", "FL", "IL", "GA", "NC", "WA"]
FIRST = ["Ava", "Liam", "Mia", "Noah", "Priya", "Arjun", "Grace", "Omar", "Sofia", "Ethan"]
LAST = ["Patel", "Garcia", "Smith", "Nguyen", "Khan", "Lopez", "Brown", "Davis"]
FLOW = {"PLACED": "PAID", "PAID": "SHIPPED", "SHIPPED": "DELIVERED"}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default="postgresql://shop:shop@localhost:5432/shop")
    p.add_argument("--ops-per-sec", type=float, default=20)
    args = p.parse_args()

    with psycopg.connect(args.dsn, autocommit=True) as conn:
        cur = conn.cursor()
        n = 0
        while True:
            r = random.random()
            if r < 0.10:
                name = f"{random.choice(FIRST)} {random.choice(LAST)}"
                cur.execute(
                    "insert into customers (email, full_name, state) values (%s, %s, %s) on conflict do nothing",
                    (f"{name.lower().replace(' ', '.')}.{random.randint(1, 10**6)}@example.com", name, random.choice(STATES)),
                )
            elif r < 0.55:
                cur.execute(
                    "insert into orders (customer_id, status, amount) "
                    "select customer_id, 'PLACED', %s from customers order by random() limit 1",
                    (round(random.uniform(8, 450), 2),),
                )
            elif r < 0.97:
                cur.execute("select order_id, status from orders where status <> 'DELIVERED' order by random() limit 1")
                row = cur.fetchone()
                if row:
                    new_status = "CANCELLED" if random.random() < 0.05 else FLOW.get(row[1], "DELIVERED")
                    cur.execute(
                        "update orders set status = %s, updated_at = now() where order_id = %s", (new_status, row[0])
                    )
            else:
                cur.execute("delete from orders where order_id = (select order_id from orders where status = 'CANCELLED' limit 1)")
            n += 1
            if n % 500 == 0:
                print(f"{n} operations")
            time.sleep(1 / args.ops_per_sec)


if __name__ == "__main__":
    main()
