"""Consumer lag — the same number you watched in L5, in its Kafka costume.

    L5:  lag_bytes = pg_current_wal_lsn() - confirmed_flush_lsn
    L6:  lag       = high watermark (log end) - committed offset, per partition

Flat lag = keeping pace. Growing lag = falling behind — and unlike L5, nobody's
disk fills when you fall behind: the log retains by time, not by acknowledgment.
The stream just waits, and this number tells on you.

Usage:
    python src/watch_lag.py                       # every 2s, forever
    python src/watch_lag.py --interval 5 --iters 12
"""

import argparse
import time

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from config import BOOTSTRAP, GROUP, TOPIC_ORDERS


def member_count(admin: AdminClient, group: str) -> int:
    try:
        fut = admin.describe_consumer_groups([group])[group]
        return len(fut.result(timeout=5).members)
    except Exception:
        return 0


def watch(group: str, topic: str, interval: float, iters: int | None) -> None:
    # A consumer that never subscribes doesn't join the group — it's just our
    # window into committed offsets and watermarks.
    probe = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"{group}-probe",
                      "enable.auto.commit": False})
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    md = probe.list_topics(topic, timeout=10)
    tps = [TopicPartition(topic, p) for p in sorted(md.topics[topic].partitions)]

    prev_total = None
    i = 0
    while iters is None or i < iters:
        committed = {tp.partition: tp.offset
                     for tp in probe.committed([TopicPartition(topic, tp.partition, 0)
                                                for tp in tps], timeout=10)}
        rows, total = [], 0
        for tp in tps:
            _, hi = probe.get_watermark_offsets(tp, timeout=10, cached=False)
            c = committed.get(tp.partition, -1001)
            lag = hi - (c if c >= 0 else 0)
            total += lag
            rows.append((tp.partition, c, hi, lag))

        if prev_total is None:
            trend = ""
        elif total > prev_total * 1.05:
            trend = "climbing"
        elif total < prev_total * 0.95:
            trend = "draining"
        else:
            trend = "steady"
        prev_total = total

        members = member_count(admin, group)
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] group={group}  consumers={members}  total lag={total:,}  {trend}")
        for partition, c, hi, lag in rows:
            bar = "#" * min(lag // 50, 50)
            print(f"   P{partition}: committed={c if c >= 0 else '-':>7}  "
                  f"hw={hi:>7}  lag={lag:>7,}  {bar}")
        print()
        i += 1
        if iters is None or i < iters:
            time.sleep(interval)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Per-partition consumer lag")
    p.add_argument("--group", default=GROUP)
    p.add_argument("--topic", default=TOPIC_ORDERS)
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--iters", type=int, default=None)
    args = p.parse_args()
    try:
        watch(args.group, args.topic, args.interval, args.iters)
    except KeyboardInterrupt:
        print("\nstopped.")
