CREATE TABLE IF NOT EXISTS fact_cs_tickets (
    ticket_id String,
    order_id Nullable (String),
    customer_email Nullable (String),
    issue_category LowCardinality (Nullable (String)),
    status LowCardinality (String),
    rating Nullable (UInt8),
    reported_at DateTime
) ENGINE = MergeTree ()
-- reported_at as the leading key supports time-range ticket queries.
-- ticket_id as the secondary key ensures unique row identification.
-- (order_id was removed from the key because it is Nullable, which ClickHouse rejects
--  in ORDER BY without allow_nullable_key = 1.)
ORDER BY (reported_at, ticket_id);