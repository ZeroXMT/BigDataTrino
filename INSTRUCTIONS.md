# Инструкция по запуску — Лабораторная работа №4 (Trino)

ETL на Trino: данные из **PostgreSQL** (файлы `MOCK_DATA (5..9).csv`) и **ClickHouse**
(файлы `MOCK_DATA.csv`, `(1..4)`) трансформируются в модель **«звезда»** в ClickHouse,
а затем — в **6 витрин-отчётов** (отдельные таблицы ClickHouse).

## Стек (docker-compose.yml)

| Сервис      | Образ                              | Порт(ы)       | Роль                                   |
|-------------|------------------------------------|---------------|----------------------------------------|
| postgres    | `postgres:18.3-trixie`             | 5432          | источник №1 (5000 строк)               |
| clickhouse  | `clickhouse/clickhouse-server:24.8`| 8123, 9000    | источник №2 + приёмник (звезда+витрины)|
| trino       | `trinodb/trino:468`                | 8080          | движок ETL поверх обоих источников     |

Trino-каталоги: `trino/catalog/postgresql.properties`, `trino/catalog/clickhouse.properties`.

## Быстрый запуск (одной командой)

```bash
cd BigDataTrino
./run.sh
```

Скрипт поднимает контейнеры, грузит CSV, прогоняет оба Trino-скрипта и печатает
проверочные счётчики (источники → звезда → витрины) и топ-5 продуктов.

## Запуск вручную (по шагам)

```bash
# 1. Контейнеры
docker compose up -d

# 2. Загрузка CSV в ClickHouse (в PostgreSQL грузится init-скриптом автоматически)
docker compose exec -T clickhouse clickhouse-client --user clickhouse --password password \
  --multiquery < clickhouse/01_staging.sql
for f in "MOCK_DATA.csv" "MOCK_DATA (1).csv" "MOCK_DATA (2).csv" "MOCK_DATA (3).csv" "MOCK_DATA (4).csv"; do
  docker compose exec -T clickhouse bash -c \
    "clickhouse-client --user clickhouse --password password \
     --query 'INSERT INTO staging.mock_data FORMAT CSVWithNames' < '/csv/$f'"
done

# 3. ETL Trino: источники -> звезда
docker compose exec -T trino trino --file /scripts/01_star_schema.sql

# 4. ETL Trino: звезда -> витрины
docker compose exec -T trino trino --file /scripts/02_reports.sql
```

## Проверка результатов

Интерактивная консоль Trino (видит оба источника и приёмник):

```bash
docker compose exec -it trino trino
```

```sql
-- таблицы звезды и витрин
SHOW TABLES FROM clickhouse.star;
SHOW TABLES FROM clickhouse.reports;

-- примеры отчётов
SELECT * FROM clickhouse.reports.mart_sales_by_product   ORDER BY revenue_rank LIMIT 10;
SELECT * FROM clickhouse.reports.mart_sales_by_time       ORDER BY year, month;
SELECT * FROM clickhouse.reports.mart_product_quality     ORDER BY rating_rank LIMIT 10;
```

Средствами ClickHouse (как требует ЛР):

```bash
docker compose exec -it clickhouse clickhouse-client --user clickhouse --password password
```

```sql
SELECT name, total_rows FROM system.tables WHERE database = 'reports' ORDER BY name;
SELECT * FROM reports.mart_sales_by_customer ORDER BY revenue_rank LIMIT 10;
```

## Скриншоты работы

Trino Web UI (**http://localhost:8080**) — кластер поднят и обслуживает ETL:

![Trino Web UI — Cluster Overview](image1.png)

Проверка отчётов в ClickHouse через DBeaver — результат `reports.mart_sales_by_product`
и ER-диаграмма всех 8 витрин схемы `reports`:

![Отчёты в ClickHouse (DBeaver)](image2.png)

## Артефакты Trino-кода

| Файл                               | Назначение                                              |
|------------------------------------|---------------------------------------------------------|
| `trino/scripts/01_star_schema.sql` | источники (PostgreSQL ∪ ClickHouse) → звезда ClickHouse |
| `trino/scripts/02_reports.sql`     | звезда ClickHouse → 6 витрин (8 таблиц) ClickHouse       |

## Модель «звезда» (схема `clickhouse.star`)

Измерения: `dim_customer`, `dim_seller`, `dim_product`, `dim_product_category`,
`dim_supplier`, `dim_pet_type`, `dim_store`, `dim_date`. Факт: `fact_sales`.

## Витрины (схема `clickhouse.reports`)

1. `mart_sales_by_product` — продажи по продуктам (+рейтинг/отзывы)
2. `mart_sales_by_customer` / `mart_customers_by_country` — по клиентам
3. `mart_sales_by_time` — помесячные и годовые тренды (`month = 0` — годовой итог)
4. `mart_sales_by_store` — по магазинам
5. `mart_sales_by_supplier` / `mart_suppliers_by_country` — по поставщикам
6. `mart_product_quality` — качество продукции (рейтинги/отзывы vs продажи)

## Сброс окружения

```bash
docker compose down -v   # удаляет контейнеры и тома (полная переустановка)
```
