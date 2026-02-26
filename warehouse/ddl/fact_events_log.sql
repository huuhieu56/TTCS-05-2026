CREATE TABLE IF NOT EXISTS fact_events_log
(
    event_id           String,
    timestamp          DateTime64(3),
    user_id            Nullable(String),
    session_id         String,
    event_type         LowCardinality(String),
    product_id         Nullable(String),
    device_os          Nullable(LowCardinality(String)),
    time_spent_seconds Nullable(UInt16)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (event_type, timestamp);
