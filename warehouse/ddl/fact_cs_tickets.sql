CREATE TABLE IF NOT EXISTS fact_cs_tickets
(
    ticket_id      String,
    order_id       Nullable(String),
    customer_email Nullable(String),
    issue_category Nullable(LowCardinality(String)),
    status         LowCardinality(String),
    rating         Nullable(UInt8),
    reported_at    DateTime
)
ENGINE = MergeTree()
ORDER BY (status, reported_at);
