"""Experiment A — Batching: use executemany / COPY to reduce round-trips.

Usage:
    python experiment_batch.py [--rows 100000] [--batch-size 1000] [--method copy]

Methods:
    executemany  — asyncpg executemany (default)
    copy         — asyncpg copy_records_to_table (fastest)
"""

import argparse
import asyncio
import os
import random
import time
from pathlib import Path

import asyncpg


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


def make_batch(size: int) -> list[tuple]:
    return [
        (random.randint(1, 10_000), round(random.uniform(1, 500), 2))
        for _ in range(size)
    ]


async def run_executemany(pool: asyncpg.Pool, total: int, batch_size: int) -> tuple[int, float]:
    done = 0
    t0 = time.monotonic()
    while done < total:
        chunk = min(batch_size, total - done)
        batch = make_batch(chunk)
        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO orders (customer_id, amount) VALUES ($1, $2)",
                batch,
            )
        done += chunk
        elapsed = time.monotonic() - t0
        tps = done / elapsed
        print(f"  [{elapsed:6.1f}s]  {tps:,.0f} TPS  |  total: {done:,}")
    return done, time.monotonic() - t0


async def run_copy(pool: asyncpg.Pool, total: int, batch_size: int) -> tuple[int, float]:
    done = 0
    t0 = time.monotonic()
    while done < total:
        chunk = min(batch_size, total - done)
        batch = make_batch(chunk)
        async with pool.acquire() as conn:
            await conn.copy_records_to_table(
                "orders",
                records=batch,
                columns=["customer_id", "amount"],
            )
        done += chunk
        elapsed = time.monotonic() - t0
        tps = done / elapsed
        print(f"  [{elapsed:6.1f}s]  {tps:,.0f} TPS  |  total: {done:,}")
    return done, time.monotonic() - t0


async def run(total_rows: int, batch_size: int, method: str) -> None:
    pool = await asyncpg.create_pool(DSN, min_size=5, max_size=5)
    print(f"Experiment A — Batching (CockroachDB): {total_rows:,} rows, batch_size={batch_size}, method={method}")
    print("-" * 60)
    if method == "executemany":
        done, elapsed = await run_executemany(pool, total_rows, batch_size)
    else:
        done, elapsed = await run_copy(pool, total_rows, batch_size)
    tps = done / elapsed
    print("-" * 60)
    print(f"Done. {done:,} rows in {elapsed:.1f}s -> {tps:,.0f} TPS")
    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Experiment A — Batching (CockroachDB)")
    parser.add_argument("--rows", "-n", type=int, default=100_000)
    parser.add_argument("--batch-size", "-b", type=int, default=1_000)
    parser.add_argument("--method", choices=["executemany", "copy"], default="executemany")
    args = parser.parse_args()
    asyncio.run(run(args.rows, args.batch_size, args.method))
