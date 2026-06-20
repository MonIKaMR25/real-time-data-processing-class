"""Phase 1 — Naive: single connection, synchronous inserts (baseline).

Usage:
    python load_naive.py [--rows 100000]
"""

import argparse
import os
import random
import time
from pathlib import Path

import psycopg


def resolve_dsn() -> str:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url
    script_path = Path(__file__).resolve()
    for base in script_path.parents:
        env_path = base / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("DATABASE_URL="):
                    return line.partition("=")[2].strip()
    return "postgresql://root@localhost:26257/bench?sslmode=disable"


def run(total_rows: int) -> None:
    print(f"Phase 1 — Naive loader (CockroachDB): {total_rows:,} rows, single connection, sync")
    print("-" * 60)

    conn = psycopg.connect(resolve_dsn())
    cur = conn.cursor()

    t0 = time.monotonic()
    report_interval = max(1, total_rows // 10)

    for i in range(1, total_rows + 1):
        cur.execute(
            "INSERT INTO orders (customer_id, amount) VALUES (%s, %s)",
            (random.randint(1, 10_000), round(random.uniform(1, 500), 2)),
        )
        if i % report_interval == 0:
            elapsed = time.monotonic() - t0
            tps = i / elapsed
            print(f"  [{elapsed:6.1f}s]  {tps:,.0f} TPS  |  total: {i:,}")

    conn.commit()
    elapsed = time.monotonic() - t0
    tps = total_rows / elapsed
    print("-" * 60)
    print(f"Done. {total_rows:,} rows in {elapsed:.1f}s -> {tps:,.0f} TPS")

    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Phase 1 — Naive sync loader")
    parser.add_argument("--rows", "-n", type=int, default=100_000)
    args = parser.parse_args()
    run(args.rows)
