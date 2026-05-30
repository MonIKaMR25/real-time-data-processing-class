"""Head-to-head benchmark: 4 queries run in both Postgres and DuckDB.

Measures wall-clock time and reports the ratio. This is the core
practical for Lesson 3 — students see the gap, then must explain it.

Usage:
    python benchmark_queries.py [--pg-only | --duck-only]
"""

import argparse
import time
from pathlib import Path

import duckdb
import psycopg

DATA_DIR = Path(__file__).parent / "data"
PG_DSN = "postgresql://bench:bench@localhost:5432/bench"
PARQUET_GLOB = str(DATA_DIR / "yellow_tripdata_2023-*.parquet")

QUERIES = [
    {
        "name": "Q1: Full aggregation (I/O bound)",
        "sql": """
            SELECT COUNT(*), AVG(fare_amount), AVG(tip_amount), AVG(trip_distance)
            FROM trips
        """,
    },
    {
        "name": "Q2: Filtered aggregation (zone maps)",
        "sql": """
            SELECT DATE_TRUNC('month', pickup_datetime) AS month,
                   payment_type,
                   COUNT(*) AS trips,
                   AVG(fare_amount) AS avg_fare,
                   SUM(tip_amount) AS total_tips
            FROM trips
            WHERE pickup_datetime >= '2023-06-01'
              AND pickup_datetime < '2023-09-01'
            GROUP BY month, payment_type
            ORDER BY month, payment_type
        """,
    },
    {
        "name": "Q3: High-cardinality GROUP BY",
        "sql": """
            SELECT pickup_location_id, dropoff_location_id,
                   COUNT(*) AS trips,
                   AVG(fare_amount) AS avg_fare
            FROM trips
            GROUP BY pickup_location_id, dropoff_location_id
            ORDER BY trips DESC
            LIMIT 20
        """,
    },
    {
        "name": "Q4: Window function (CPU bound)",
        "sql": """
            SELECT pickup_location_id, pickup_datetime, fare_amount,
                   AVG(fare_amount) OVER (
                       PARTITION BY pickup_location_id
                       ORDER BY pickup_datetime
                       ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
                   ) AS rolling_avg
            FROM trips
            WHERE pickup_location_id = 132
            ORDER BY pickup_datetime
        """,
    },
]

# DuckDB reads from Parquet — replace 'trips' with the glob
DUCK_TABLE = f"'{PARQUET_GLOB}'"


def run_postgres(sql: str) -> float:
    """Run query in Postgres, return wall-clock seconds."""
    with psycopg.connect(PG_DSN) as conn:
        t0 = time.monotonic()
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.fetchall()
        return time.monotonic() - t0


def run_duckdb(sql: str) -> float:
    """Run query in DuckDB, return wall-clock seconds."""
    con = duckdb.connect()
    # Replace 'trips' table reference with Parquet glob
    duck_sql = sql.replace("trips", DUCK_TABLE)
    # DuckDB uses tpep_pickup_datetime / tpep_dropoff_datetime in raw Parquet
    duck_sql = duck_sql.replace("pickup_datetime", "tpep_pickup_datetime")
    duck_sql = duck_sql.replace("dropoff_datetime", "tpep_dropoff_datetime")
    duck_sql = duck_sql.replace("pickup_location_id", "PULocationID")
    duck_sql = duck_sql.replace("dropoff_location_id", "DOLocationID")

    t0 = time.monotonic()
    con.sql(duck_sql).fetchall()
    elapsed = time.monotonic() - t0
    con.close()
    return elapsed


def main(pg_only: bool = False, duck_only: bool = False) -> None:
    print("=" * 70)
    print("  Lesson 3 — DuckDB vs Postgres: Head-to-Head Benchmark")
    print("=" * 70)
    print()

    results = []

    for q in QUERIES:
        print(f"  {q['name']}")
        print(f"  {'-' * 50}")

        pg_time = None
        duck_time = None

        if not duck_only:
            try:
                pg_time = run_postgres(q["sql"])
                print(f"    Postgres:  {pg_time:.3f}s")
            except Exception as e:
                print(f"    Postgres:  ERROR — {e}")

        if not pg_only:
            try:
                duck_time = run_duckdb(q["sql"])
                print(f"    DuckDB:    {duck_time:.3f}s")
            except Exception as e:
                print(f"    DuckDB:    ERROR — {e}")

        if pg_time and duck_time:
            ratio = pg_time / duck_time
            print(f"    Ratio:     {ratio:.1f}× faster in DuckDB")

        results.append({"name": q["name"], "pg": pg_time, "duck": duck_time})
        print()

    # Summary table
    print("=" * 70)
    print(f"  {'Query':<40} {'Postgres':>10} {'DuckDB':>10} {'Ratio':>8}")
    print(f"  {'-'*40} {'-'*10} {'-'*10} {'-'*8}")
    for r in results:
        pg_str = f"{r['pg']:.2f}s" if r["pg"] else "—"
        duck_str = f"{r['duck']:.3f}s" if r["duck"] else "—"
        ratio_str = f"{r['pg']/r['duck']:.0f}×" if r["pg"] and r["duck"] else "—"
        print(f"  {r['name']:<40} {pg_str:>10} {duck_str:>10} {ratio_str:>8}")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lesson 3: DuckDB vs Postgres benchmark")
    parser.add_argument("--pg-only", action="store_true", help="Only run Postgres")
    parser.add_argument("--duck-only", action="store_true", help="Only run DuckDB")
    args = parser.parse_args()
    main(pg_only=args.pg_only, duck_only=args.duck_only)
