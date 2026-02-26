CREATE TABLE IF NOT EXISTS customer_360_view
(
    user_id                String,
    full_name              String,
    customer_city          LowCardinality(String),
    customer_state         LowCardinality(String),
    loyalty_tier           Nullable(LowCardinality(String)),
    total_lifetime_value   Decimal(18, 2),
    total_orders_completed UInt32,
    total_abandoned_carts  UInt32,
    total_cs_complaints    UInt32,
    last_active_date       Nullable(DateTime),
    churn_risk_score       Nullable(Float32)
)
ENGINE = MergeTree()
ORDER BY user_id;
