-- prod's own Reverse Sync target, so a local prod run never writes the table stage writes.
CREATE DATABASE shop_prod OWNER shop;

\connect shop_prod

CREATE TABLE customer_stats (
    customer_id integer PRIMARY KEY,
    order_count bigint NOT NULL,
    total_amount numeric(12, 2) NOT NULL,
    last_order_at timestamptz NOT NULL
);
