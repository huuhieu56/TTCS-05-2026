CREATE TABLE IF NOT EXISTS fact_order_items
(
    item_id    String,
    order_id   String,
    product_id String,
    quantity   UInt16,
    unit_price Decimal(12, 2)
)
ENGINE = MergeTree()
ORDER BY (order_id, product_id);
