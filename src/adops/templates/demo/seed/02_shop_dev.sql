-- dev's own Reverse Sync target, so a dev Preview never reads the table stage writes.
CREATE DATABASE shop_dev OWNER shop;

\connect shop_dev

CREATE TABLE customer_stats (
    customer_id integer PRIMARY KEY,
    order_count bigint NOT NULL,
    total_amount numeric(12, 2) NOT NULL,
    last_order_at timestamptz NOT NULL
);
