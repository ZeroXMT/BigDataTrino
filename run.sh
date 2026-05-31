#!/usr/bin/env bash
# Полный запуск ETL-пайплайна Trino:
#   1) поднимает PostgreSQL + ClickHouse + Trino
#   2) грузит исходные CSV (5 файлов в PostgreSQL, 5 — в ClickHouse)
#   3) прогоняет Trino-скрипты: источники -> звезда -> 6 витрин (всё в ClickHouse)
#   4) выводит проверочные счётчики
set -euo pipefail

CH="docker compose exec -T clickhouse clickhouse-client --user clickhouse --password password"

echo "=== [1/6] Поднимаем контейнеры ==="
docker compose up -d

echo "=== [2/6] Ждём готовности PostgreSQL и ClickHouse ==="
until docker compose exec -T postgres pg_isready -U postgres -d mydatabase -q; do sleep 3; done
echo "  PostgreSQL ready (файлы (5)-(9) загружены init-скриптом)"
until docker compose exec -T clickhouse wget --spider -q http://localhost:8123/ping 2>/dev/null; do sleep 3; done
echo "  ClickHouse ready"

echo "=== [3/6] ClickHouse: создаём staging и грузим файлы MOCK_DATA/(1)-(4) ==="
$CH --multiquery < clickhouse/01_staging.sql
for f in "MOCK_DATA.csv" "MOCK_DATA (1).csv" "MOCK_DATA (2).csv" "MOCK_DATA (3).csv" "MOCK_DATA (4).csv"; do
    echo "  -> $f"
    docker compose exec -T clickhouse bash -c \
      "clickhouse-client --user clickhouse --password password \
       --query 'INSERT INTO staging.mock_data FORMAT CSVWithNames' < '/csv/$f'"
done
ch_rows=$($CH --query "SELECT count() FROM staging.mock_data")
echo "  ClickHouse: строк в staging.mock_data: $ch_rows"

echo "=== [4/6] Ждём готовности Trino ==="
until docker compose exec -T trino trino --execute "SELECT 1" >/dev/null 2>&1; do sleep 5; done
echo "  Trino ready"
echo "  Проверка коннективности каталогов:"
docker compose exec -T trino trino --execute \
  "SELECT 'postgresql' AS catalog, count(*) AS rows FROM postgresql.public.mock_data
   UNION ALL
   SELECT 'clickhouse', count(*) FROM clickhouse.staging.mock_data"

echo "=== [5/6] Trino ETL: источники -> звезда -> витрины ==="
echo "  -> 01_star_schema.sql"
docker compose exec -T trino trino --file /scripts/01_star_schema.sql
echo "  -> 02_reports.sql"
docker compose exec -T trino trino --file /scripts/02_reports.sql

echo ""
echo "=== [6/6] Проверка результатов ==="
echo "--- Источники ---"
pg_rows=$(docker compose exec -T postgres psql -At -U postgres -d mydatabase -c "SELECT count(*) FROM mock_data;")
echo "  PostgreSQL mock_data : $pg_rows"
echo "  ClickHouse mock_data : $ch_rows"

echo "--- Звезда (clickhouse.star) ---"
$CH --query "
SELECT name, total_rows
FROM system.tables
WHERE database = 'star'
ORDER BY name
FORMAT PrettyCompactMonoBlock"

echo "--- Витрины (clickhouse.reports) ---"
$CH --query "
SELECT name, total_rows
FROM system.tables
WHERE database = 'reports'
ORDER BY name
FORMAT PrettyCompactMonoBlock"

echo ""
echo "--- Топ-5 продуктов по выручке (mart_sales_by_product) ---"
$CH --query "
SELECT revenue_rank, product_name, category_name, total_revenue, total_quantity
FROM reports.mart_sales_by_product
ORDER BY revenue_rank
LIMIT 5
FORMAT PrettyCompactMonoBlock"

echo ""
echo "Готово! ETL Trino выполнен, звезда и 8 витрин в ClickHouse."
