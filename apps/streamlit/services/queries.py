"""Pre-built SQL queries for the Customer 360 dashboard.

Queries that accept filters are parameterized with ClickHouse native
parameters {name:Type} to avoid SQL injection.
"""

# ---------------------------------------------------------------------------
# Overview KPIs
# ---------------------------------------------------------------------------

TOTAL_CUSTOMERS = "SELECT count() FROM dim_users"
TOTAL_ORDERS = "SELECT count() FROM fact_orders"
TOTAL_REVENUE = "SELECT sum(total_amount) FROM fact_orders WHERE order_status = 'Completed'"
TOTAL_EVENTS = "SELECT count() FROM fact_events_log"
TOTAL_TICKETS = "SELECT count() FROM fact_cs_tickets"
TOTAL_PRODUCTS = "SELECT count() FROM dim_products"

# ---------------------------------------------------------------------------
# Revenue over time — dynamic granularity (day / month / year)
# ---------------------------------------------------------------------------

REVENUE_BY_DAY = """
SELECT
    toDate(created_at) AS period,
    sum(total_amount)  AS revenue,
    count()            AS order_count
FROM fact_orders
WHERE order_status = 'Completed'
GROUP BY period
ORDER BY period
"""

REVENUE_BY_MONTH = """
SELECT
    toStartOfMonth(created_at) AS period,
    sum(total_amount)          AS revenue,
    count()                    AS order_count
FROM fact_orders
WHERE order_status = 'Completed'
GROUP BY period
ORDER BY period
"""

REVENUE_BY_YEAR = """
SELECT
    toStartOfYear(created_at) AS period,
    sum(total_amount)         AS revenue,
    count()                   AS order_count
FROM fact_orders
WHERE order_status = 'Completed'
GROUP BY period
ORDER BY period
"""

# ---------------------------------------------------------------------------
# Orders by status — optionally filtered by month
# ---------------------------------------------------------------------------

ORDERS_BY_STATUS = """
SELECT
    order_status,
    count() AS cnt
FROM fact_orders
GROUP BY order_status
ORDER BY cnt DESC
"""

ORDERS_BY_STATUS_IN_MONTH = """
SELECT
    order_status,
    count() AS cnt
FROM fact_orders
WHERE toStartOfMonth(created_at) = {month:Date}
GROUP BY order_status
ORDER BY cnt DESC
"""

# ---------------------------------------------------------------------------
# Revenue by payment method — optionally filtered by month
# ---------------------------------------------------------------------------

REVENUE_BY_PAYMENT = """
SELECT
    payment_method,
    sum(total_amount) AS revenue,
    count()           AS order_count
FROM fact_orders
WHERE payment_method IS NOT NULL
GROUP BY payment_method
ORDER BY revenue DESC
"""

REVENUE_BY_PAYMENT_IN_MONTH = """
SELECT
    payment_method,
    sum(total_amount) AS revenue,
    count()           AS order_count
FROM fact_orders
WHERE payment_method IS NOT NULL
  AND toStartOfMonth(created_at) = {month:Date}
GROUP BY payment_method
ORDER BY revenue DESC
"""

# ---------------------------------------------------------------------------
# Top 10 products by revenue — optionally filtered by month
# ---------------------------------------------------------------------------

TOP_PRODUCTS = """
SELECT
    p.product_name,
    p.category,
    sum(oi.unit_price * oi.quantity) AS revenue,
    sum(oi.quantity)                 AS units_sold
FROM fact_order_items oi
JOIN dim_products p ON oi.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 10
"""

TOP_PRODUCTS_IN_MONTH = """
SELECT
    p.product_name,
    p.category,
    sum(oi.unit_price * oi.quantity) AS revenue,
    sum(oi.quantity)                 AS units_sold
FROM fact_order_items oi
JOIN dim_products p ON oi.product_id = p.product_id
JOIN fact_orders o ON oi.order_id = o.order_id
WHERE toStartOfMonth(o.created_at) = {month:Date}
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 10
"""

# ---------------------------------------------------------------------------
# Customer loyalty distribution
# ---------------------------------------------------------------------------

LOYALTY_DISTRIBUTION = """
SELECT
    if(loyalty_tier IS NULL, 'No Tier', loyalty_tier) AS tier,
    count() AS cnt
FROM dim_users
GROUP BY tier
ORDER BY
    CASE tier
        WHEN 'Platinum' THEN 1
        WHEN 'Gold' THEN 2
        WHEN 'Silver' THEN 3
        WHEN 'Bronze' THEN 4
        ELSE 5
    END
"""

# ---------------------------------------------------------------------------
# Clickstream events by type — optionally filtered by month
# ---------------------------------------------------------------------------

EVENTS_BY_TYPE = """
SELECT
    event_type,
    count() AS cnt
FROM fact_events_log
GROUP BY event_type
ORDER BY cnt DESC
"""

EVENTS_BY_TYPE_IN_MONTH = """
SELECT
    event_type,
    count() AS cnt
FROM fact_events_log
WHERE toStartOfMonth(timestamp) = {month:Date}
GROUP BY event_type
ORDER BY cnt DESC
"""

# ---------------------------------------------------------------------------
# Events over time (daily)
# ---------------------------------------------------------------------------

EVENTS_BY_DAY = """
SELECT
    toDate(timestamp) AS day,
    event_type,
    count()           AS cnt
FROM fact_events_log
GROUP BY day, event_type
ORDER BY day
"""

EVENTS_BY_DAY_IN_MONTH = """
SELECT
    toDate(timestamp) AS day,
    event_type,
    count()           AS cnt
FROM fact_events_log
WHERE toStartOfMonth(timestamp) = {month:Date}
GROUP BY day, event_type
ORDER BY day
"""

# ---------------------------------------------------------------------------
# Device OS distribution
# ---------------------------------------------------------------------------

DEVICE_DISTRIBUTION = """
SELECT
    if(device_os IS NULL, 'Unknown', device_os) AS device_os,
    count() AS cnt
FROM fact_events_log
GROUP BY device_os
ORDER BY cnt DESC
"""

# ---------------------------------------------------------------------------
# CS tickets by category — optionally filtered by month
# ---------------------------------------------------------------------------

TICKETS_BY_CATEGORY = """
SELECT
    if(issue_category IS NULL, 'Uncategorized', issue_category) AS category,
    count() AS cnt
FROM fact_cs_tickets
GROUP BY category
ORDER BY cnt DESC
"""

TICKETS_BY_CATEGORY_IN_MONTH = """
SELECT
    if(issue_category IS NULL, 'Uncategorized', issue_category) AS category,
    count() AS cnt
FROM fact_cs_tickets
WHERE toStartOfMonth(reported_at) = {month:Date}
GROUP BY category
ORDER BY cnt DESC
"""

# ---------------------------------------------------------------------------
# Tickets by status
# ---------------------------------------------------------------------------

TICKETS_BY_STATUS = """
SELECT
    status,
    count() AS cnt
FROM fact_cs_tickets
GROUP BY status
ORDER BY cnt DESC
"""

TICKETS_BY_STATUS_IN_MONTH = """
SELECT
    status,
    count() AS cnt
FROM fact_cs_tickets
WHERE toStartOfMonth(reported_at) = {month:Date}
GROUP BY status
ORDER BY cnt DESC
"""

# ---------------------------------------------------------------------------
# Available months (for filter dropdowns)
# ---------------------------------------------------------------------------

AVAILABLE_ORDER_MONTHS = """
SELECT DISTINCT toStartOfMonth(created_at) AS month
FROM fact_orders
ORDER BY month DESC
"""

AVAILABLE_EVENT_MONTHS = """
SELECT DISTINCT toStartOfMonth(timestamp) AS month
FROM fact_events_log
ORDER BY month DESC
"""

AVAILABLE_TICKET_MONTHS = """
SELECT DISTINCT toStartOfMonth(reported_at) AS month
FROM fact_cs_tickets
ORDER BY month DESC
"""

# ---------------------------------------------------------------------------
# Customer 360 table (paginated)
# ---------------------------------------------------------------------------

CUSTOMER_360_TABLE = """
SELECT
    user_id,
    full_name,
    customer_city,
    customer_state,
    loyalty_tier,
    total_lifetime_value,
    total_orders_completed,
    total_abandoned_carts,
    total_cs_complaints,
    last_active_date
FROM customer_360
ORDER BY total_lifetime_value DESC
LIMIT {limit:UInt32} OFFSET {offset:UInt32}
"""

CUSTOMER_360_COUNT = "SELECT count() FROM customer_360"

CUSTOMER_360_SEARCH = """
SELECT
    user_id,
    full_name,
    customer_city,
    customer_state,
    loyalty_tier,
    total_lifetime_value,
    total_orders_completed,
    total_abandoned_carts,
    total_cs_complaints,
    last_active_date
FROM customer_360
WHERE full_name ILIKE {search:String}
   OR user_id ILIKE {search:String}
ORDER BY total_lifetime_value DESC
LIMIT 50
"""

# ---------------------------------------------------------------------------
# Top states by revenue
# ---------------------------------------------------------------------------

TOP_STATES = """
SELECT
    u.customer_state,
    sum(o.total_amount) AS revenue,
    count()             AS order_count
FROM fact_orders o
JOIN dim_users u ON o.user_id = u.user_id
WHERE o.order_status = 'Completed'
GROUP BY u.customer_state
ORDER BY revenue DESC
LIMIT 10
"""

# ---------------------------------------------------------------------------
# Tickets trend over time (daily)
# ---------------------------------------------------------------------------

TICKETS_BY_DAY = """
SELECT
    toDate(reported_at) AS day,
    count()             AS cnt
FROM fact_cs_tickets
GROUP BY day
ORDER BY day
"""

TICKETS_BY_DAY_IN_MONTH = """
SELECT
    toDate(reported_at) AS day,
    count()             AS cnt
FROM fact_cs_tickets
WHERE toStartOfMonth(reported_at) = {month:Date}
GROUP BY day
ORDER BY day
"""

# ---------------------------------------------------------------------------
# Average rating over time
# ---------------------------------------------------------------------------

AVG_RATING_BY_MONTH = """
SELECT
    toStartOfMonth(reported_at) AS month,
    round(avg(rating), 2)       AS avg_rating,
    count()                     AS ticket_count
FROM fact_cs_tickets
WHERE rating IS NOT NULL
GROUP BY month
ORDER BY month
"""
