#!/usr/bin/env bash
# Загружает в staging-таблицу mock_data ВТОРУЮ половину исходных файлов:
# mock_data (5)..(9) — 5 файлов по 1000 строк = 5000 строк.
# Первая половина (MOCK_DATA.csv, (1)..(4)) грузится в ClickHouse (см. run.sh).
# Запускается один раз при первом старте контейнера (docker-entrypoint-initdb.d).
set -euo pipefail

echo "PostgreSQL: загрузка файлов (5)-(9) в mock_data ..."

for n in 5 6 7 8 9; do
    f="/csv/MOCK_DATA (${n}).csv"
    [ -e "$f" ] || { echo "  ПРОПУЩЕН (нет файла): $f"; continue; }
    echo "  -> $f"
    psql -v ON_ERROR_STOP=1 \
         --username "$POSTGRES_USER" \
         --dbname  "$POSTGRES_DB" \
         -c "\copy mock_data FROM '$f' WITH (FORMAT csv, HEADER true, NULL '')"
done

count=$(psql -At --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        -c "SELECT count(*) FROM mock_data;")
echo "PostgreSQL: строк в mock_data: $count"
