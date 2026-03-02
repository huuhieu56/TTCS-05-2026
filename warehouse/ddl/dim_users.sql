CREATE TABLE IF NOT EXISTS dim_users (
    user_id String,
    full_name String,
    email Nullable (String),
    phone_number Nullable (String),
    customer_city LowCardinality (String),
    customer_state LowCardinality (String),
    loyalty_tier LowCardinality (Nullable (String)),
    created_at DateTime
) ENGINE = MergeTree ()
ORDER BY user_id;