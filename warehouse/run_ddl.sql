CREATE DATABASE IF NOT EXISTS ttcs;

USE ttcs;

-- 1) Dimension tables
SOURCE 'ddl/dim_users.sql';
SOURCE 'ddl/dim_products.sql';

-- 2) Fact tables
SOURCE 'ddl/fact_orders.sql';
SOURCE 'ddl/fact_order_items.sql';
SOURCE 'ddl/fact_events_log.sql';
SOURCE 'ddl/fact_cs_tickets.sql';

-- 3) Aggregation / Wide table
SOURCE 'views/customer_360_view.sql';
