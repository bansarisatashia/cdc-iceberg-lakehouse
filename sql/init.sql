-- OLTP source schema for the CDC demo (e-commerce orders)

CREATE TABLE IF NOT EXISTS customers (
    customer_id   SERIAL PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    full_name     TEXT NOT NULL,
    state         CHAR(2),
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id      SERIAL PRIMARY KEY,
    customer_id   INT REFERENCES customers(customer_id),
    status        TEXT NOT NULL,
    amount        NUMERIC(12,2) NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;
CREATE PUBLICATION dbz_publication FOR TABLE customers, orders;
