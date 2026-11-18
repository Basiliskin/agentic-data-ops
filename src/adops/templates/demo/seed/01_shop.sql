-- Synthetic demo data. Every value is computed from the row number, so each fresh start is identical.
CREATE TABLE customers (
    id integer PRIMARY KEY,
    email text,
    updated_at timestamptz NOT NULL
);

CREATE TABLE orders (
    id integer PRIMARY KEY,
    customer_id integer NOT NULL,
    amount numeric(10, 2) NOT NULL,
    updated_at timestamptz NOT NULL
);

INSERT INTO customers (id, email, updated_at)
SELECT n,
       'customer' || n || '@example.com',
       TIMESTAMPTZ '2026-01-01 00:00:00+00' + n * INTERVAL '1 hour'
FROM generate_series(1, 20) AS n;

INSERT INTO orders (id, customer_id, amount, updated_at)
SELECT n,
       (n - 1) % 20 + 1,
       ((n * 37) % 500 + 5) + (n % 100) / 100.0,
       TIMESTAMPTZ '2026-02-01 00:00:00+00' + n * INTERVAL '10 minutes'
FROM generate_series(1, 100) AS n;
