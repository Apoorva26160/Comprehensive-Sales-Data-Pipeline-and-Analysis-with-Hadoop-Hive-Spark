-- ─────────────────────────────────────────────────────────────────────────────
-- Sales Analysis — Hive SQL Queries
-- Assumes data is loaded into Hive tables from the PySpark pipeline output.
-- ─────────────────────────────────────────────────────────────────────────────

-- Create external Hive table over Parquet output
CREATE EXTERNAL TABLE IF NOT EXISTS sales_clean (
    order_id        STRING,
    order_date      DATE,
    customer_id     STRING,
    customer_segment STRING,
    product_id      STRING,
    category        STRING,
    sub_category    STRING,
    region          STRING,
    quantity        INT,
    unit_price      DOUBLE,
    discount        DOUBLE,
    revenue         DOUBLE,
    profit          DOUBLE,
    discount_tier   STRING,
    year            INT,
    month           INT,
    quarter         INT
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/sales_clean';


-- ── QUERY 1: Monthly Revenue Trend ───────────────────────────────────────────
SELECT
    year,
    month,
    ROUND(SUM(revenue), 2)                              AS monthly_revenue,
    ROUND(SUM(profit), 2)                               AS monthly_profit,
    ROUND(SUM(profit) / NULLIF(SUM(revenue), 0) * 100, 2) AS profit_margin_pct,
    COUNT(DISTINCT order_id)                            AS num_orders
FROM sales_clean
GROUP BY year, month
ORDER BY year, month;


-- ── QUERY 2: Regional Performance ────────────────────────────────────────────
SELECT
    region,
    ROUND(SUM(revenue), 2)           AS total_revenue,
    ROUND(AVG(revenue), 2)           AS avg_order_revenue,
    COUNT(DISTINCT customer_id)      AS unique_customers,
    COUNT(order_id)                  AS total_orders,
    ROUND(SUM(profit), 2)            AS total_profit
FROM sales_clean
GROUP BY region
ORDER BY total_revenue DESC;


-- ── QUERY 3: Category & Sub-category Breakdown ───────────────────────────────
SELECT
    category,
    sub_category,
    ROUND(SUM(revenue), 2)  AS revenue,
    SUM(quantity)           AS units_sold,
    ROUND(AVG(discount), 3) AS avg_discount,
    ROUND(SUM(profit), 2)   AS profit
FROM sales_clean
GROUP BY category, sub_category
ORDER BY revenue DESC;


-- ── QUERY 4: Discount Impact Analysis ────────────────────────────────────────
SELECT
    discount_tier,
    COUNT(order_id)          AS num_orders,
    ROUND(AVG(revenue), 2)   AS avg_revenue_per_order,
    ROUND(AVG(profit), 2)    AS avg_profit_per_order,
    ROUND(SUM(revenue), 2)   AS total_revenue
FROM sales_clean
GROUP BY discount_tier
ORDER BY total_revenue DESC;


-- ── QUERY 5: Top 10 Customers by Revenue ─────────────────────────────────────
SELECT
    customer_id,
    customer_segment,
    ROUND(SUM(revenue), 2)   AS total_revenue,
    COUNT(order_id)          AS num_orders,
    ROUND(AVG(revenue), 2)   AS avg_order_value,
    MIN(order_date)          AS first_order,
    MAX(order_date)          AS last_order
FROM sales_clean
GROUP BY customer_id, customer_segment
ORDER BY total_revenue DESC
LIMIT 10;


-- ── QUERY 6: YoY Growth ───────────────────────────────────────────────────────
SELECT
    curr.year,
    curr.region,
    ROUND(curr.revenue, 2)                                              AS current_revenue,
    ROUND(prev.revenue, 2)                                              AS prev_revenue,
    ROUND((curr.revenue - prev.revenue) / NULLIF(prev.revenue, 0) * 100, 2) AS yoy_growth_pct
FROM (
    SELECT year, region, SUM(revenue) AS revenue
    FROM sales_clean
    GROUP BY year, region
) curr
LEFT JOIN (
    SELECT year, region, SUM(revenue) AS revenue
    FROM sales_clean
    GROUP BY year, region
) prev ON curr.region = prev.region AND curr.year = prev.year + 1
ORDER BY curr.year, curr.region;
