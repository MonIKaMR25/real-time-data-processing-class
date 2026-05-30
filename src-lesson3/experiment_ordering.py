"""Experiment C: Data ordering impact on zone maps.

Creates two copies of the trips data in DuckDB:
  - trips_by_time: sorted by pickup_datetime (zone maps effective)
  - trips_shuffled: random order (zone maps useless)

Then runs the same date-filtered query on both and compares.

Usage:
    python experiment_ordering.py
"""

import time
from pathlib import Path

import duckdb

DATA_DIR = Path(__file__).parent / "data"
PARQUET_GLOB = str(DATA_DIR / "yellow_tripdata_2023-*.parquet")
DB_PATH = str(DATA_DIR / "ordering_experiment.duckdb")

QUERY = """
    SELECT DATE_TRUNC('month', tpep_pickup_datetime) AS month,
           AVG(fare_amount) AS avg_fare
    FROM {table}
    WHERE tpep_pickup_datetime >= '2023-06-01'
      AND tpep_pickup_datetime < '2023-09-01'
    GROUP BY month
    ORDER BY month
"""


def setup(con: duckdb.DuckDBPyConnection) -> None:
    """Create sorted and shuffled tables if they don't exist."""
    tables = [r[0] for r in con.sql("SHOW TABLES").fetchall()]

    if "trips_by_time" not in tables:
        print("  Creating trips_by_time (sorted by pickup_datetime)...")
        t0 = time.monotonic()
        con.sql(f"""
            CREATE TABLE trips_by_time AS
            SELECT * FROM '{PARQUET_GLOB}'
            ORDER BY tpep_pickup_datetime
        """)
        print(f"    Done in {time.monotonic() - t0:.1f}s")
    else:
        print("  trips_by_time already exists")

    if "trips_shuffled" not in tables:
        print("  Creating trips_shuffled (random order)...")
        t0 = time.monotonic()
        con.sql(f"""
            CREATE TABLE trips_shuffled AS
            SELECT * FROM '{PARQUET_GLOB}'
            ORDER BY RANDOM()
        """)
        print(f"    Done in {time.monotonic() - t0:.1f}s")
    else:
        print("  trips_shuffled already exists")


def bench_query(con: duckdb.DuckDBPyConnection, table: str) -> float:
    """Run filtered query, return wall-clock seconds."""
    sql = QUERY.format(table=table)
    t0 = time.monotonic()
    con.sql(sql).fetchall()
    return time.monotonic() - t0


def main() -> None:
    print("Experiment C — Data Ordering Impact on Zone Maps")
    print("=" * 60)

    con = duckdb.connect(DB_PATH)
    setup(con)
    print()

    # Warm up
    con.sql("SELECT COUNT(*) FROM trips_by_time").fetchone()

    # Benchmark sorted
    print("  Query: AVG(fare) by month WHERE pickup in Jun-Sep 2023")
    print("-" * 60)

    times_sorted = []
    for i in range(3):
        t = bench_query(con, "trips_by_time")
        times_sorted.append(t)

    times_shuffled = []
    for i in range(3):
        t = bench_query(con, "trips_shuffled")
        times_shuffled.append(t)

    avg_sorted = sum(times_sorted) / len(times_sorted)
    avg_shuffled = sum(times_shuffled) / len(times_shuffled)

    print(f"  trips_by_time (sorted):    {avg_sorted:.3f}s (avg of 3 runs)")
    print(f"  trips_shuffled (random):   {avg_shuffled:.3f}s (avg of 3 runs)")
    print()

    if avg_shuffled > avg_sorted:
        ratio = avg_shuffled / avg_sorted
        print(f"  Sorted is {ratio:.1f}× faster — zone maps skip entire row groups")
    else:
        print("  Unexpected: shuffled was faster (likely cached)")

    print()
    print("  Why: sorted data → date range maps to contiguous row groups → skip")
    print("       shuffled → every row group spans all dates → no skipping")
    print()
    print("  Use EXPLAIN ANALYZE on both to see row groups scanned vs skipped:")
    print("    EXPLAIN ANALYZE SELECT ... FROM trips_by_time WHERE ...")
    print("    EXPLAIN ANALYZE SELECT ... FROM trips_shuffled WHERE ...")

    con.close()


if __name__ == "__main__":
    main()
