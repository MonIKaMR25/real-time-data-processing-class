"""Load NYC Taxi Parquet data into Postgres using DuckDB as the converter.

DuckDB reads Parquet natively and exports to CSV which Postgres can COPY.
This demonstrates the loading cost asymmetry: minutes for Postgres vs. zero for DuckDB.

Usage:
    python load_postgres.py [--limit 1000000]
"""

import argparse
import subprocess
import time
from pathlib import Path

import duckdb

DATA_DIR = Path(__file__).parent / "data"
PG_DSN = "postgresql://bench:bench@localhost:5432/bench"


def load(limit: int | None = None) -> None:
    parquet_glob = str(DATA_DIR / "yellow_tripdata_2023-*.parquet")

    print("Loading NYC Taxi data into Postgres...")
    print(f"  Source: {parquet_glob}")
    if limit:
        print(f"  Limit: {limit:,} rows")

    # Use DuckDB to read Parquet and pipe CSV into Postgres COPY
    con = duckdb.connect()

    # Count total rows available
    total = con.sql(f"SELECT COUNT(*) FROM '{parquet_glob}'").fetchone()[0]
    print(f"  Total rows in Parquet: {total:,}")

    rows_to_load = limit or total
    print(f"  Loading: {rows_to_load:,} rows")
    print()

    t0 = time.monotonic()

    # Export to a temp CSV, then COPY into Postgres
    # (More robust than piping for large datasets)
    csv_path = DATA_DIR / "_tmp_load.csv"
    query = f"""
        COPY (
            SELECT
                VendorID as vendor_id,
                tpep_pickup_datetime as pickup_datetime,
                tpep_dropoff_datetime as dropoff_datetime,
                passenger_count,
                trip_distance,
                PULocationID as pickup_location_id,
                DOLocationID as dropoff_location_id,
                RatecodeID as rate_code_id,
                payment_type,
                fare_amount,
                extra,
                mta_tax,
                tip_amount,
                tolls_amount,
                total_amount,
                congestion_surcharge,
                airport_fee
            FROM '{parquet_glob}'
            LIMIT {rows_to_load}
        ) TO '{csv_path}' (HEADER, DELIMITER ',')
    """
    con.sql(query)
    con.close()

    export_time = time.monotonic() - t0
    print(f"  DuckDB export to CSV: {export_time:.1f}s")

    # COPY CSV into Postgres
    t1 = time.monotonic()
    cmd = [
        "psql", PG_DSN, "-c",
        f"\\COPY trips FROM '{csv_path}' WITH (FORMAT csv, HEADER true)"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        return

    copy_time = time.monotonic() - t1
    total_time = time.monotonic() - t0

    print(f"  Postgres COPY: {copy_time:.1f}s")
    print(f"  Total: {total_time:.1f}s for {rows_to_load:,} rows")
    print(f"  Rate: {rows_to_load / total_time:,.0f} rows/sec")

    # Cleanup
    csv_path.unlink(missing_ok=True)

    # ANALYZE for query planner
    print("  Running ANALYZE...")
    subprocess.run(["psql", PG_DSN, "-c", "ANALYZE trips;"], capture_output=True)
    print("  Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Parquet into Postgres")
    parser.add_argument("--limit", "-l", type=int, default=None,
                        help="Limit rows to load (default: all)")
    args = parser.parse_args()
    load(args.limit)
