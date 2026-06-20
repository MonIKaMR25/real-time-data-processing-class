"""The hook: what the SECOND consumer costs when the OLTP source is your buffer.

Runs against the LESSON 5 Postgres (it must be up: cd ../src-lesson5 && docker
compose up -d). Two new teams each want the change stream, so each gets its own
replication slot. One team's consumer keeps pace (we advance its slot); the
other team's consumer is "down for maintenance" — i.e. the realistic case.

Watch pg_replication_slots while a write workload runs:
  - the consumed slot stays near zero retained WAL
  - the stalled slot pins WAL on the SOURCE, growing without bound
  - every additional reader = another slot = another independent time bomb,
    and every slot decodes the WAL separately (CPU on the source, again)

The fix is not "monitor harder." It's moving the buffer OFF the source: a log
in the middle that retains by time and lets readers track their own positions.

Usage:
    python src/experiment_second_slot.py                 # ~20s, then cleans up
    python src/experiment_second_slot.py --seconds 30
"""

import argparse
import time

import psycopg

from config import PG_DSN

SLOTS = ["team_fraud", "team_search"]      # the two new teams
SCRATCH = "l6_hook_traffic"                # our own table: don't touch L5's data

LAG_SQL = """
    SELECT slot_name,
           pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained,
           active
    FROM pg_replication_slots
    WHERE slot_name = ANY(%s)
    ORDER BY slot_name
"""


def run(seconds: int) -> None:
    with psycopg.connect(PG_DSN, autocommit=True) as pg:
        print("two new teams want the CDC stream -> two new slots on the SOURCE:\n")
        for slot in SLOTS:
            pg.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'wal2json')"
                " WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
                (slot, slot),
            )
            print(f"  created slot {slot}")
        pg.execute(f"CREATE TABLE IF NOT EXISTS {SCRATCH} (id BIGINT, payload TEXT)")

        print(f"\nwrite workload for {seconds}s — {SLOTS[0]}'s consumer keeps pace,"
              f" {SLOTS[1]}'s is down:\n")
        t0 = time.time()
        i = 0
        while time.time() - t0 < seconds:
            # the workload: bulk-ish inserts so the WAL actually moves
            pg.execute(
                f"INSERT INTO {SCRATCH} SELECT g, repeat('x', 200) "
                f"FROM generate_series(%s, %s) g", (i, i + 999),
            )
            i += 1000
            # team_fraud's consumer is alive: drain + advance its slot
            pg.execute(
                "SELECT count(*) FROM pg_logical_slot_get_changes(%s, NULL, NULL)",
                (SLOTS[0],),
            )
            if i % 5000 == 0:
                rows = pg.execute(LAG_SQL, (SLOTS,)).fetchall()
                ts = time.strftime("%H:%M:%S")
                stat = "   ".join(f"{name}: {retained} retained" for name, retained, _ in rows)
                print(f"  [{ts}] rows={i:>7,}   {stat}")

        print("\nfinal state:")
        for name, retained, active in pg.execute(LAG_SQL, (SLOTS,)).fetchall():
            note = "" if active else "   <- nobody reading; this NEVER shrinks"
            print(f"  {name:<14} retained WAL: {retained}{note}")

        print("\ncleanup (dropping slots + scratch table)...")
        for slot in SLOTS:
            pg.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
        pg.execute(f"DROP TABLE IF EXISTS {SCRATCH}")
        print("done. The source survived because we remembered to clean up. "
              "Production forgets.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="The cost of reader #2 on the source")
    p.add_argument("--seconds", type=int, default=20)
    args = p.parse_args()
    run(args.seconds)
