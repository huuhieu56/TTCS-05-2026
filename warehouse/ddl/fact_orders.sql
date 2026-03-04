CREATE TABLE IF NOT EXISTS fact_orders (
    order_id String,
    user_id Nullable (String),
    total_amount Decimal(18, 2),
    order_status LowCardinality (String),
    payment_method LowCardinality (Nullable (String)),
    created_at DateTime
) ENGINE = MergeTree ()
PARTITION BY
    toYYYYMM (created_at)
    -- created_at as the leading key enables partition pruning and time-range scans.
    -- order_id as the secondary key ensures unique row identification within a time slice.
    -- (user_id was removed from the key because it is Nullable, which ClickHouse rejects
    --  in ORDER BY without allow_nullable_key = 1.)
ORDER BY (created_at, order_id);