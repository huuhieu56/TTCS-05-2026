CREATE TABLE IF NOT EXISTS dim_products (
    product_id String,
    product_name String,
    category LowCardinality (Nullable (String)),
    cost_price Decimal(10, 2)
) ENGINE = MergeTree ()
ORDER BY product_id;