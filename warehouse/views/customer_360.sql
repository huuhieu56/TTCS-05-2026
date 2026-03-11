CREATE TABLE IF NOT EXISTS customer_360 (
    user_id String,
    full_name String,
    customer_city LowCardinality (String),
    customer_state LowCardinality (String),
    loyalty_tier LowCardinality (Nullable (String)),
    total_lifetime_value Decimal(18, 2),
    total_orders_completed UInt32,
    total_abandoned_carts UInt32,
    total_cs_complaints UInt32,
    last_active_date Nullable (DateTime),
    churn_risk_score Nullable (Float32)
) ENGINE = ReplacingMergeTree ()
ORDER BY user_id;