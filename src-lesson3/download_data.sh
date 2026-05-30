#!/usr/bin/env bash
# Download NYC Taxi Yellow 2023 Parquet files
# ~38M rows for the full year. Combine with 2022 for ~76M rows.

set -euo pipefail

DATA_DIR="$(dirname "$0")/data"
mkdir -p "$DATA_DIR"

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

echo "Downloading 2023 Yellow Taxi data..."
for month in $(seq -w 1 12); do
    FILE="yellow_tripdata_2023-${month}.parquet"
    if [ -f "$DATA_DIR/$FILE" ]; then
        echo "  ✓ $FILE (already exists)"
    else
        echo "  ↓ $FILE"
        curl -sL "$BASE_URL/$FILE" -o "$DATA_DIR/$FILE"
    fi
done

echo ""
echo "Done. Files in $DATA_DIR/"
ls -lh "$DATA_DIR"/*.parquet | awk '{print "  " $5 "\t" $NF}'
echo ""
echo "Total rows (approx): ~38M for 2023"
echo "To get ~100M rows, also download 2022:"
echo "  for m in \$(seq -w 1 12); do curl -sL $BASE_URL/yellow_tripdata_2022-\$m.parquet -o $DATA_DIR/yellow_tripdata_2022-\$m.parquet; done"
