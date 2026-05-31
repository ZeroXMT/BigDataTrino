-- =====================================================================
--  Trino ETL #1: источники (PostgreSQL + ClickHouse) -> звезда (ClickHouse)
--
--  Источники:
--    postgresql.public.mock_data        — файлы (5)-(9), 5000 строк
--    clickhouse.staging.mock_data       — файлы MOCK_DATA/(1)-(4), 5000 строк
--  Приёмник:
--    clickhouse.star.*                  — модель «звезда» (8 dim + 1 fact)
--
--  Запуск: trino --file /scripts/01_star_schema.sql
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS clickhouse.star;

-- Идемпотентность: чистим результаты предыдущего прогона
DROP TABLE IF EXISTS clickhouse.star.fact_sales;
DROP TABLE IF EXISTS clickhouse.star.dim_product;
DROP TABLE IF EXISTS clickhouse.star.dim_supplier;
DROP TABLE IF EXISTS clickhouse.star.dim_product_category;
DROP TABLE IF EXISTS clickhouse.star.dim_customer;
DROP TABLE IF EXISTS clickhouse.star.dim_pet_type;
DROP TABLE IF EXISTS clickhouse.star.dim_seller;
DROP TABLE IF EXISTS clickhouse.star.dim_store;
DROP TABLE IF EXISTS clickhouse.star.dim_date;
DROP TABLE IF EXISTS clickhouse.staging.src;

-- ---------------------------------------------------------------------
-- 1. Единый типизированный источник.
--    UNION ALL обеих БД + однократное приведение типов/дат.
--    Материализуем в ClickHouse, чтобы дальнейшие шаги шли локально.
-- ---------------------------------------------------------------------
CREATE TABLE clickhouse.staging.src
WITH (engine = 'MergeTree', order_by = ARRAY['row_id']) AS
WITH unioned AS (
    SELECT * FROM postgresql.public.mock_data
    UNION ALL
    SELECT * FROM clickhouse.staging.mock_data
)
SELECT
    CAST(row_number() OVER (ORDER BY id) AS bigint)                      AS row_id,
    COALESCE(TRY_CAST(NULLIF(sale_customer_id, '') AS integer), 0)       AS customer_id,
    customer_first_name,
    customer_last_name,
    TRY_CAST(NULLIF(customer_age, '') AS integer)                       AS customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    COALESCE(NULLIF(pet_category, ''), '')                              AS pet_category,
    customer_pet_name,
    customer_pet_breed,
    COALESCE(TRY_CAST(NULLIF(sale_seller_id, '') AS integer), 0)        AS seller_id,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code,
    COALESCE(TRY_CAST(NULLIF(sale_product_id, '') AS integer), 0)       AS product_id,
    product_name,
    product_category,
    TRY_CAST(NULLIF(product_price, '') AS double)                      AS product_price,
    TRY(CAST(date_parse(NULLIF(sale_date, ''), '%c/%e/%Y') AS date))   AS sale_date,
    TRY_CAST(NULLIF(sale_quantity, '') AS integer)                     AS sale_quantity,
    TRY_CAST(NULLIF(sale_total_price, '') AS double)                   AS sale_total_price,
    store_name,
    store_location,
    COALESCE(NULLIF(store_city, ''), '')                               AS store_city,
    store_state,
    store_country,
    store_phone,
    store_email,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    TRY_CAST(NULLIF(product_rating, '') AS double)                     AS product_rating,
    TRY_CAST(NULLIF(product_reviews, '') AS integer)                   AS product_reviews,
    product_release_date,
    product_expiry_date,
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    COALESCE(NULLIF(supplier_city, ''), '')                            AS supplier_city,
    supplier_country
FROM unioned;

-- ---------------------------------------------------------------------
-- 2. dim_pet_type (суррогатный ключ)
-- ---------------------------------------------------------------------
CREATE TABLE clickhouse.star.dim_pet_type
WITH (engine = 'MergeTree', order_by = ARRAY['pet_type_id']) AS
SELECT
    CAST(row_number() OVER (ORDER BY pet_type, pet_category) AS integer) AS pet_type_id,
    pet_type,
    pet_category
FROM (
    SELECT DISTINCT customer_pet_type AS pet_type, pet_category
    FROM clickhouse.staging.src
    WHERE customer_pet_type IS NOT NULL AND customer_pet_type <> ''
);

-- ---------------------------------------------------------------------
-- 3. dim_seller (натуральный ключ seller_id)
-- ---------------------------------------------------------------------
CREATE TABLE clickhouse.star.dim_seller
WITH (engine = 'MergeTree', order_by = ARRAY['seller_id']) AS
SELECT seller_id, first_name, last_name, email, country, postal_code
FROM (
    SELECT
        seller_id,
        seller_first_name AS first_name,
        seller_last_name  AS last_name,
        seller_email      AS email,
        seller_country    AS country,
        seller_postal_code AS postal_code,
        row_number() OVER (PARTITION BY seller_id ORDER BY row_id) AS rn
    FROM clickhouse.staging.src
    WHERE seller_id <> 0
)
WHERE rn = 1;

-- ---------------------------------------------------------------------
-- 4. dim_customer (натуральный ключ customer_id, ссылка на dim_pet_type)
-- ---------------------------------------------------------------------
CREATE TABLE clickhouse.star.dim_customer
WITH (engine = 'MergeTree', order_by = ARRAY['customer_id']) AS
SELECT
    c.customer_id, c.first_name, c.last_name, c.age, c.email,
    c.country, c.postal_code, pt.pet_type_id, c.pet_name, c.pet_breed
FROM (
    SELECT
        customer_id,
        customer_first_name AS first_name,
        customer_last_name  AS last_name,
        customer_age        AS age,
        customer_email      AS email,
        customer_country    AS country,
        customer_postal_code AS postal_code,
        customer_pet_type,
        pet_category,
        customer_pet_name  AS pet_name,
        customer_pet_breed AS pet_breed,
        row_number() OVER (PARTITION BY customer_id ORDER BY row_id) AS rn
    FROM clickhouse.staging.src
    WHERE customer_id <> 0
) c
LEFT JOIN clickhouse.star.dim_pet_type pt
    ON c.customer_pet_type = pt.pet_type
   AND c.pet_category      = pt.pet_category
WHERE c.rn = 1;

-- ---------------------------------------------------------------------
-- 5. dim_product_category (суррогатный ключ)
-- ---------------------------------------------------------------------
CREATE TABLE clickhouse.star.dim_product_category
WITH (engine = 'MergeTree', order_by = ARRAY['category_id']) AS
SELECT
    CAST(row_number() OVER (ORDER BY category_name) AS integer) AS category_id,
    category_name
FROM (
    SELECT DISTINCT product_category AS category_name
    FROM clickhouse.staging.src
    WHERE product_category IS NOT NULL AND product_category <> ''
);

-- ---------------------------------------------------------------------
-- 6. dim_supplier (суррогатный ключ, дедуп по name+city)
-- ---------------------------------------------------------------------
CREATE TABLE clickhouse.star.dim_supplier
WITH (engine = 'MergeTree', order_by = ARRAY['supplier_id']) AS
SELECT
    CAST(row_number() OVER (ORDER BY name, city) AS integer) AS supplier_id,
    name, contact, email, phone, address, city, country
FROM (
    SELECT
        supplier_name    AS name,
        supplier_contact AS contact,
        supplier_email   AS email,
        supplier_phone   AS phone,
        supplier_address AS address,
        supplier_city    AS city,
        supplier_country AS country,
        row_number() OVER (PARTITION BY supplier_name, supplier_city ORDER BY row_id) AS rn
    FROM clickhouse.staging.src
    WHERE supplier_name IS NOT NULL AND supplier_name <> ''
)
WHERE rn = 1;

-- ---------------------------------------------------------------------
-- 7. dim_store (суррогатный ключ, дедуп по name+city)
-- ---------------------------------------------------------------------
CREATE TABLE clickhouse.star.dim_store
WITH (engine = 'MergeTree', order_by = ARRAY['store_id']) AS
SELECT
    CAST(row_number() OVER (ORDER BY name, city) AS integer) AS store_id,
    name, location, city, state, country, phone, email
FROM (
    SELECT
        store_name     AS name,
        store_location AS location,
        store_city     AS city,
        store_state    AS state,
        store_country  AS country,
        store_phone    AS phone,
        store_email    AS email,
        row_number() OVER (PARTITION BY store_name, store_city ORDER BY row_id) AS rn
    FROM clickhouse.staging.src
    WHERE store_name IS NOT NULL AND store_name <> ''
)
WHERE rn = 1;

-- ---------------------------------------------------------------------
-- 8. dim_date (суррогатный ключ, развёртка компонентов даты)
-- ---------------------------------------------------------------------
CREATE TABLE clickhouse.star.dim_date
WITH (engine = 'MergeTree', order_by = ARRAY['date_id']) AS
SELECT
    CAST(row_number() OVER (ORDER BY full_date) AS integer) AS date_id,
    full_date,
    CAST(day(full_date)     AS integer) AS day,
    CAST(month(full_date)   AS integer) AS month,
    CAST(year(full_date)    AS integer) AS year,
    CAST(quarter(full_date) AS integer) AS quarter
FROM (
    SELECT DISTINCT sale_date AS full_date
    FROM clickhouse.staging.src
    WHERE sale_date IS NOT NULL
);

-- ---------------------------------------------------------------------
-- 9. dim_product (натуральный ключ product_id, ссылки на category/supplier)
-- ---------------------------------------------------------------------
CREATE TABLE clickhouse.star.dim_product
WITH (engine = 'MergeTree', order_by = ARRAY['product_id']) AS
SELECT
    p.product_id, p.name, cat.category_id, sup.supplier_id,
    p.price, p.weight, p.color, p.size, p.brand, p.material, p.description,
    p.rating, p.reviews, p.release_date, p.expiry_date
FROM (
    SELECT
        product_id,
        product_name        AS name,
        product_category,
        supplier_name,
        supplier_city,
        product_price        AS price,
        product_weight       AS weight,
        product_color        AS color,
        product_size         AS size,
        product_brand        AS brand,
        product_material     AS material,
        product_description  AS description,
        product_rating       AS rating,
        product_reviews      AS reviews,
        product_release_date AS release_date,
        product_expiry_date  AS expiry_date,
        row_number() OVER (PARTITION BY product_id ORDER BY row_id) AS rn
    FROM clickhouse.staging.src
    WHERE product_id <> 0
) p
LEFT JOIN clickhouse.star.dim_product_category cat
    ON p.product_category = cat.category_name
LEFT JOIN clickhouse.star.dim_supplier sup
    ON p.supplier_name = sup.name
   AND p.supplier_city = sup.city
WHERE p.rn = 1;

-- ---------------------------------------------------------------------
-- 10. fact_sales (зерно — одна продажа; суррогатный sale_id)
-- ---------------------------------------------------------------------
CREATE TABLE clickhouse.star.fact_sales
WITH (engine = 'MergeTree', order_by = ARRAY['sale_id']) AS
SELECT
    CAST(row_number() OVER (ORDER BY f.row_id) AS bigint) AS sale_id,
    d.date_id,
    f.customer_id,
    f.seller_id,
    f.product_id,
    st.store_id,
    f.sale_quantity    AS quantity,
    f.sale_total_price AS total_price
FROM clickhouse.staging.src f
LEFT JOIN clickhouse.star.dim_date d
    ON f.sale_date = d.full_date
LEFT JOIN clickhouse.star.dim_store st
    ON f.store_name = st.name
   AND f.store_city = st.city
WHERE f.sale_date IS NOT NULL;
