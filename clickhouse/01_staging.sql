-- Staging-таблица ClickHouse, повторяющая структуру исходного CSV (50 колонок).
-- В ClickHouse загружаются файлы MOCK_DATA.csv и (1)-(4) — всего 5000 строк.
-- Все поля String (кроме id), чтобы UNION ALL с PostgreSQL-источником в Trino
-- сходился по типам, а парсинг/каст значений выполнялся уже на этапе ETL.

CREATE DATABASE IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.mock_data;

CREATE TABLE staging.mock_data
(
    id                   Int32,
    customer_first_name  String,
    customer_last_name   String,
    customer_age         String,
    customer_email       String,
    customer_country     String,
    customer_postal_code String,
    customer_pet_type    String,
    customer_pet_name    String,
    customer_pet_breed   String,
    seller_first_name    String,
    seller_last_name     String,
    seller_email         String,
    seller_country       String,
    seller_postal_code   String,
    product_name         String,
    product_category     String,
    product_price        String,
    product_quantity     String,
    sale_date            String,
    sale_customer_id     String,
    sale_seller_id       String,
    sale_product_id      String,
    sale_quantity        String,
    sale_total_price     String,
    store_name           String,
    store_location       String,
    store_city           String,
    store_state          String,
    store_country        String,
    store_phone          String,
    store_email          String,
    pet_category         String,
    product_weight       String,
    product_color        String,
    product_size         String,
    product_brand        String,
    product_material     String,
    product_description  String,
    product_rating       String,
    product_reviews      String,
    product_release_date String,
    product_expiry_date  String,
    supplier_name        String,
    supplier_contact     String,
    supplier_email       String,
    supplier_phone       String,
    supplier_address     String,
    supplier_city        String,
    supplier_country     String
)
ENGINE = MergeTree()
ORDER BY id;
