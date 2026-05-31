-- =====================================================================
--  Trino ETL #2: звезда (ClickHouse) -> 6 витрин-отчётов (ClickHouse)
--
--  Каждый отчёт — отдельная таблица в схеме clickhouse.reports.
--  Запуск: trino --file /scripts/02_reports.sql
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS clickhouse.reports;

DROP TABLE IF EXISTS clickhouse.reports.mart_sales_by_product;
DROP TABLE IF EXISTS clickhouse.reports.mart_sales_by_customer;
DROP TABLE IF EXISTS clickhouse.reports.mart_customers_by_country;
DROP TABLE IF EXISTS clickhouse.reports.mart_sales_by_time;
DROP TABLE IF EXISTS clickhouse.reports.mart_sales_by_store;
DROP TABLE IF EXISTS clickhouse.reports.mart_sales_by_supplier;
DROP TABLE IF EXISTS clickhouse.reports.mart_suppliers_by_country;
DROP TABLE IF EXISTS clickhouse.reports.mart_product_quality;

-- ======== Отчёт 1: Витрина продаж по продуктам ========
-- Топ продуктов по выручке, выручка по категориям, средний рейтинг/отзывы.
CREATE TABLE clickhouse.reports.mart_sales_by_product
WITH (engine = 'MergeTree', order_by = ARRAY['revenue_rank']) AS
SELECT
    p.product_id,
    p.name                                  AS product_name,
    COALESCE(pc.category_name, 'N/A')       AS category_name,
    ROUND(SUM(f.total_price), 2)            AS total_revenue,
    SUM(f.quantity)                         AS total_quantity,
    ROUND(AVG(COALESCE(p.rating, 0)), 2)    AS avg_rating,
    MAX(COALESCE(p.reviews, 0))             AS total_reviews,
    CAST(RANK() OVER (ORDER BY SUM(f.total_price) DESC) AS bigint) AS revenue_rank
FROM clickhouse.star.fact_sales f
JOIN clickhouse.star.dim_product p ON f.product_id = p.product_id
LEFT JOIN clickhouse.star.dim_product_category pc ON p.category_id = pc.category_id
GROUP BY p.product_id, p.name, pc.category_name;

-- ======== Отчёт 2: Витрина продаж по клиентам ========
-- Топ-клиенты по сумме покупок, средний чек, количество заказов.
CREATE TABLE clickhouse.reports.mart_sales_by_customer
WITH (engine = 'MergeTree', order_by = ARRAY['revenue_rank']) AS
SELECT
    c.customer_id,
    CONCAT(COALESCE(c.first_name, ''), ' ', COALESCE(c.last_name, '')) AS customer_name,
    COALESCE(c.country, 'Unknown')          AS country,
    COUNT(f.sale_id)                        AS total_orders,
    ROUND(SUM(f.total_price), 2)            AS total_revenue,
    ROUND(AVG(f.total_price), 2)            AS avg_basket,
    CAST(RANK() OVER (ORDER BY SUM(f.total_price) DESC) AS bigint) AS revenue_rank
FROM clickhouse.star.fact_sales f
JOIN clickhouse.star.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.country;

-- ======== Отчёт 2 (доп.): Распределение клиентов по странам ========
CREATE TABLE clickhouse.reports.mart_customers_by_country
WITH (engine = 'MergeTree', order_by = ARRAY['country_rank']) AS
SELECT
    COALESCE(c.country, 'Unknown')          AS country,
    COUNT(DISTINCT c.customer_id)           AS customer_count,
    COUNT(f.sale_id)                        AS total_orders,
    ROUND(SUM(f.total_price), 2)            AS total_revenue,
    ROUND(AVG(f.total_price), 2)            AS avg_basket,
    CAST(RANK() OVER (ORDER BY COUNT(DISTINCT c.customer_id) DESC) AS bigint) AS country_rank
FROM clickhouse.star.fact_sales f
JOIN clickhouse.star.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.country;

-- ======== Отчёт 3: Витрина продаж по времени ========
-- month = 0 — годовые итоги; остальные строки — помесячные.
CREATE TABLE clickhouse.reports.mart_sales_by_time
WITH (engine = 'MergeTree', order_by = ARRAY['year', 'month']) AS
SELECT
    d.year,
    d.month,
    'monthly'                       AS period_type,
    COUNT(f.sale_id)                AS total_orders,
    ROUND(SUM(f.total_price), 2)    AS total_revenue,
    ROUND(AVG(f.total_price), 2)    AS avg_order_value
FROM clickhouse.star.fact_sales f
JOIN clickhouse.star.dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
UNION ALL
SELECT
    d.year,
    CAST(0 AS integer)              AS month,
    'yearly'                        AS period_type,
    COUNT(f.sale_id)                AS total_orders,
    ROUND(SUM(f.total_price), 2)    AS total_revenue,
    ROUND(AVG(f.total_price), 2)    AS avg_order_value
FROM clickhouse.star.fact_sales f
JOIN clickhouse.star.dim_date d ON f.date_id = d.date_id
GROUP BY d.year;

-- ======== Отчёт 4: Витрина продаж по магазинам ========
CREATE TABLE clickhouse.reports.mart_sales_by_store
WITH (engine = 'MergeTree', order_by = ARRAY['revenue_rank']) AS
SELECT
    st.store_id,
    st.name                                 AS store_name,
    COALESCE(st.city, 'Unknown')            AS city,
    COALESCE(st.country, 'Unknown')         AS country,
    COUNT(f.sale_id)                        AS total_orders,
    ROUND(SUM(f.total_price), 2)            AS total_revenue,
    ROUND(AVG(f.total_price), 2)            AS avg_basket,
    CAST(RANK() OVER (ORDER BY SUM(f.total_price) DESC) AS bigint) AS revenue_rank
FROM clickhouse.star.fact_sales f
JOIN clickhouse.star.dim_store st ON f.store_id = st.store_id
GROUP BY st.store_id, st.name, st.city, st.country;

-- ======== Отчёт 5: Витрина продаж по поставщикам ========
CREATE TABLE clickhouse.reports.mart_sales_by_supplier
WITH (engine = 'MergeTree', order_by = ARRAY['revenue_rank']) AS
SELECT
    s.supplier_id,
    s.name                                  AS supplier_name,
    COALESCE(s.country, 'Unknown')          AS country,
    ROUND(SUM(f.total_price), 2)            AS total_revenue,
    ROUND(AVG(p.price), 2)                  AS avg_item_price,
    COUNT(DISTINCT p.product_id)            AS product_count,
    CAST(RANK() OVER (ORDER BY SUM(f.total_price) DESC) AS bigint) AS revenue_rank
FROM clickhouse.star.fact_sales f
JOIN clickhouse.star.dim_product p  ON f.product_id  = p.product_id
JOIN clickhouse.star.dim_supplier s ON p.supplier_id = s.supplier_id
GROUP BY s.supplier_id, s.name, s.country;

-- ======== Отчёт 5 (доп.): Распределение продаж по странам поставщиков ========
CREATE TABLE clickhouse.reports.mart_suppliers_by_country
WITH (engine = 'MergeTree', order_by = ARRAY['country_rank']) AS
SELECT
    COALESCE(s.country, 'Unknown')          AS supplier_country,
    COUNT(DISTINCT s.supplier_id)           AS supplier_count,
    COUNT(DISTINCT p.product_id)            AS product_count,
    ROUND(SUM(f.total_price), 2)            AS total_revenue,
    CAST(RANK() OVER (ORDER BY SUM(f.total_price) DESC) AS bigint) AS country_rank
FROM clickhouse.star.fact_sales f
JOIN clickhouse.star.dim_product p  ON f.product_id  = p.product_id
JOIN clickhouse.star.dim_supplier s ON p.supplier_id = s.supplier_id
GROUP BY s.country;

-- ======== Отчёт 6: Витрина качества продукции ========
-- Рейтинги/отзывы и их связь с объёмом продаж.
CREATE TABLE clickhouse.reports.mart_product_quality
WITH (engine = 'MergeTree', order_by = ARRAY['rating_rank']) AS
SELECT
    p.product_id,
    p.name                                  AS product_name,
    COALESCE(pc.category_name, 'N/A')       AS category_name,
    COALESCE(p.rating, 0)                   AS rating,
    COALESCE(p.reviews, 0)                  AS reviews,
    COUNT(f.sale_id)                        AS sales_count,
    ROUND(SUM(f.total_price), 2)            AS total_revenue,
    CAST(RANK() OVER (ORDER BY COALESCE(p.rating, 0) DESC) AS bigint) AS rating_rank
FROM clickhouse.star.fact_sales f
JOIN clickhouse.star.dim_product p ON f.product_id = p.product_id
LEFT JOIN clickhouse.star.dim_product_category pc ON p.category_id = pc.category_id
GROUP BY p.product_id, p.name, pc.category_name, p.rating, p.reviews;
