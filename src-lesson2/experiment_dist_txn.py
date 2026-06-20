"""Experiment A (Hour 3) — Distributed transactions across ranges.

Forces 2PC overhead by transferring money between accounts on different ranges.
Compares single-range vs. cross-range transaction throughput.

Usage:
    python experiment_dist_txn.py [--connections 10] [--duration 30]
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

TRANSFER_SQL = """
UPDATE accounts SET balance = balance - $1, version = version + 1 WHERE id = $2;
UPDATE accounts SET balance = balance + $1, version = version + 1 WHERE id = $3;
"""


async def ensure_accounts(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPSERT INTO accounts (id, balance) VALUES (1, 10000), (2, 10000), (3, 10000), (4, 10000)"
        )


async def run(connections: int, duration: int) -> None:
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)
    await ensure_accounts(pool)

    print(f"Experiment — Distributed transactions: {connections} connections, {duration}s")
    print("-" * 60)
    print("Running transfers between accounts (may span different ranges).")
    print()

    done = 0
    errors = 0
    stats_lock = asyncio.Lock()
    running = True
    t0 = time.monotonic()

    async def reporter():
        nonlocal done, errors
        while running:
            await asyncio.sleep(2.0)
            elapsed = time.monotonic() - t0
            async with stats_lock:
                current = done
                errs = errors
            if current > 0:
                tps = current / elapsed
                print(f"  [{elapsed:6.1f}s]  {tps:,.0f} TPS  |  committed: {current:,}  errors: {errs}")

    async def transfer_worker():
        nonlocal done, errors
        while running:
            try:
                async with pool.acquire() as conn:
                    for attempt in range(3):
                        try:
                            async with conn.transaction():
                                from_id = random.randint(1, 4)
                                to_id = random.randint(1, 4)
                                while to_id == from_id:
                                    to_id = random.randint(1, 4)
                                amount = round(random.uniform(1, 100), 2)
                                await conn.execute(
                                    "UPDATE accounts SET balance = balance - $1, version = version + 1 WHERE id = $2",
                                    amount, from_id,
                                )
                                await conn.execute(
                                    "UPDATE accounts SET balance = balance + $1, version = version + 1 WHERE id = $2",
                                    amount, to_id,
                                )
                            break
                        except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                            if e.sqlstate not in CRDB_RETRY_CODES:
                                raise
                            backoff = 0.01 * (2 ** attempt) + random.uniform(0, 0.01)
                            await asyncio.sleep(backoff)
                async with stats_lock:
                    done += 1
            except Exception:
                async with stats_lock:
                    errors += 1
                await asyncio.sleep(0.1)

    reporter_task = asyncio.create_task(reporter())
    tasks = [asyncio.create_task(transfer_worker()) for _ in range(connections)]

    await asyncio.sleep(duration)
    running = False

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    reporter_task.cancel()

    elapsed = time.monotonic() - t0
    tps = done / elapsed
    print("-" * 60)
    print(f"Done. {done:,} transfers in {elapsed:.1f}s -> {tps:,.0f} TPS")
    print(f"Errors: {errors}")
    print()
    print("Compare this to single-range TPS from load_async.py.")
    print("Hint: in CockroachDB Admin UI (http://localhost:8080), check")
    print("'Ranges' and 'Raft' metrics to see the effect of 2PC overhead.")
    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed transaction benchmark (CockroachDB)")
    parser.add_argument("--connections", "-c", type=int, default=10)
    parser.add_argument("--duration", "-d", type=int, default=30)
    args = parser.parse_args()
    asyncio.run(run(args.connections, args.duration))
