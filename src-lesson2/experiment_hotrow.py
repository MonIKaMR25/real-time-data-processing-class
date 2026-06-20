"""Experiment C — Hot row: all coroutines UPDATE the same row.

CockroachDB handles hot rows differently than Postgres thanks to
transaction retries and serializable isolation.

Usage:
    python experiment_hotrow.py [--connections 50] [--duration 30]
"""

import argparse
import asyncio
import os
import random
import time
from pathlib import Path

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


async def ensure_target_row(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        row = await conn.fetchval("SELECT id FROM accounts WHERE id = 1")
        if row is None:
            await conn.execute("INSERT INTO accounts (id, balance) VALUES (1, 10000)")


async def run(connections: int, duration: int) -> None:
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)
    await ensure_target_row(pool)

    print(f"Experiment C — Hot row (CockroachDB): {connections} connections, all UPDATE id=1, {duration}s")
    print("-" * 60)

    done = 0
    done_lock = asyncio.Lock()
    running = True
    t0 = time.monotonic()

    async def reporter():
        nonlocal done
        while running:
            await asyncio.sleep(1.0)
            elapsed = time.monotonic() - t0
            async with done_lock:
                current = done
            if current > 0:
                tps = current / elapsed
                print(f"  [{elapsed:6.1f}s]  {tps:,.0f} TPS  |  total: {current:,}")

    async def contender():
        nonlocal done
        while running:
            try:
                async with pool.acquire() as conn:
                    for attempt in range(3):
                        try:
                            await conn.execute(
                                "UPDATE accounts SET balance = balance + $1, version = version + 1 WHERE id = 1",
                                round(random.uniform(0.01, 1.0), 2),
                            )
                            break
                        except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                            if e.sqlstate not in CRDB_RETRY_CODES:
                                raise
                            backoff = 0.005 * (2 ** attempt) + random.uniform(0, 0.005)
                            await asyncio.sleep(backoff)
                async with done_lock:
                    done += 1
            except Exception:
                await asyncio.sleep(0.01)

    reporter_task = asyncio.create_task(reporter())
    tasks = [asyncio.create_task(contender()) for _ in range(connections)]

    await asyncio.sleep(duration)
    running = False

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    reporter_task.cancel()

    elapsed = time.monotonic() - t0
    tps = done / elapsed
    print("-" * 60)
    print(f"Done. {done:,} updates in {elapsed:.1f}s -> {tps:,.0f} TPS")
    print("Compare this to INSERT throughput at the same concurrency level.")
    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Experiment C — Hot row contention (CockroachDB)")
    parser.add_argument("--connections", "-c", type=int, default=50)
    parser.add_argument("--duration", "-d", type=int, default=30)
    args = parser.parse_args()
    asyncio.run(run(args.connections, args.duration))
