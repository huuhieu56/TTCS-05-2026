"""Pre-built SQL queries for the Customer 360 dashboard."""

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
# Revenue over time (monthly)
# ---------------------------------------------------------------------------

REVENUE_BY_MONTH = """
SELECT
    toStartOfMonth(created_at) AS month,
    sum(total_amount)          AS revenue,
    count()                    AS order_count
FROM fact_orders
WHERE order_status = 'Completed'
GROUP BY month
ORDER BY month
"""

# ---------------------------------------------------------------------------
# Orders by status
# ---------------------------------------------------------------------------

ORDERS_BY_STATUS = """
SELECT
    order_status,
    count() AS cnt
FROM fact_orders
GROUP BY order_status
ORDER BY cnt DESC
"""

# ---------------------------------------------------------------------------
# Revenue by payment method
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

# ---------------------------------------------------------------------------
# Top 10 products by revenue
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
# Clickstream events by type
# ---------------------------------------------------------------------------

EVENTS_BY_TYPE = """
SELECT
    event_type,
    count() AS cnt
FROM fact_events_log
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
# CS tickets by category
# ---------------------------------------------------------------------------

TICKETS_BY_CATEGORY = """
SELECT
    if(issue_category IS NULL, 'Uncategorized', issue_category) AS category,
    count() AS cnt
FROM fact_cs_tickets
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
LIMIT {limit} OFFSET {offset}
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
