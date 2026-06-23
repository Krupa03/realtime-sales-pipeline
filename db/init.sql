CREATE TABLE IF NOT EXISTS orders (
    order_id    TEXT,
    customer_id TEXT,
    product     TEXT,
    quantity    INT,
    price       NUMERIC(10,2),
    region      TEXT,
    status      TEXT,
    order_time  TIMESTAMPTZ NOT NULL
);

SELECT create_hypertable('orders', 'order_time', if_not_exists => TRUE);

CREATE MATERIALIZED VIEW IF NOT EXISTS orders_per_minute
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', order_time) AS bucket,
       product,
       region,
       COUNT(*)                             AS order_count,
       SUM(price * quantity)               AS revenue
FROM orders
GROUP BY bucket, product, region;