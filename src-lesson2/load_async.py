"""Phase 2 — Async: asyncpg connection pool with N concurrent coroutines.

Ported from Lesson 1 with CockroachDB retry handling for serializable isolation.

Usage:
    python load_async.py [--connections 50] [--rows 100000] [--mode insert]

Modes:
    insert  — INSERT new rows (default)
    update  — UPDATE random existing rows (needs pre-loaded data)
"""

import argparse
import asyncio
import os
from pathlib import Path
import random
import time

import asyncpg


CRDB_RETRY_CODES = {"40001", "CR000"}


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


DSN = resolve_dsn()

INSERT_SQL = "INSERT INTO orders (customer_id, amount) VALUES ($1, $2)"
UPDATE_SQL = "UPDATE orders SET amount = amount + $1 WHERE id IN (SELECT id FROM orders ORDER BY random() LIMIT 1)"


async def crdb_execute(conn: asyncpg.Connection, sql: str, *args, max_retries: int = 3) -> None:
    for attempt in range(max_retries):
        try:
            await conn.execute(sql, *args)
            return
        except asyncpg.exceptions.IntegrityConstraintViolationError as e:
            if e.sqlstate in CRDB_RETRY_CODES:
                backoff = 0.01 * (2 ** attempt) + random.uniform(0, 0.01)
                await asyncio.sleep(backoff)
                continue
            raise
        except asyncpg.exceptions.ForeignKeyViolationError as e:
            if e.sqlstate in CRDB_RETRY_CODES:
                backoff = 0.01 * (2 ** attempt) + random.uniform(0, 0.01)
                await asyncio.sleep(backoff)
                continue
            raise


async def run(connections: int, total_rows: int, mode: str) -> None:
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)

    label = f"Phase 2 — Async loader (CockroachDB): {total_rows:,} rows, {connections} connections, mode={mode}"
    print(label)
    print("-" * 60)

    counter = {"done": 0}
    t0 = time.monotonic()
    rows_per_worker = total_rows // connections
    remainder = total_rows % connections

    async def reporter():
        while True:
            await asyncio.sleep(1.0)
            elapsed = time.monotonic() - t0
            current = counter["done"]
            if current > 0:
                tps = current / elapsed
                print(f"  [{elapsed:6.1f}s]  {tps:,.0f} TPS  |  total: {current:,}")
            if current >= total_rows:
                break

    async def worker(worker_id: int, n_rows: int):
        conn = await pool.acquire()
        try:
            for _ in range(n_rows):
                if mode == "insert":
                    await crdb_execute(conn, INSERT_SQL, random.randint(1, 10_000), round(random.uniform(1, 500), 2))
                else:
                    await crdb_execute(conn, UPDATE_SQL, round(random.uniform(0.01, 1.0), 2))
                counter["done"] += 1
        finally:
            await pool.release(conn)

    reporter_task = asyncio.create_task(reporter())
    tasks = []
    for i in range(connections):
        n = rows_per_worker + (1 if i < remainder else 0)
        tasks.append(asyncio.create_task(worker(i, n)))
    await asyncio.gather(*tasks)
    await reporter_task

    elapsed = time.monotonic() - t0
    tps = total_rows / elapsed
    print("-" * 60)
    print(f"Done. {total_rows:,} rows in {elapsed:.1f}s -> {tps:,.0f} TPS")

    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Phase 2 — Async loader (CockroachDB)")
    parser.add_argument("--connections", "-c", type=int, default=50)
    parser.add_argument("--rows", "-n", type=int, default=100_000)
    parser.add_argument("--mode", choices=["insert", "update"], default="insert")
    args = parser.parse_args()
    asyncio.run(run(args.connections, args.rows, args.mode))
