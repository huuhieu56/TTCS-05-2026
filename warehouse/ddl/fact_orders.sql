CREATE TABLE IF NOT EXISTS fact_orders
(
    order_id       String,
    user_id        Nullable(String),
    total_amount   Decimal(18, 2),
    order_status   LowCardinality(String),
    payment_method Nullable(LowCardinality(String)),
    created_at     DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (order_status, created_at);
