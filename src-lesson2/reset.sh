#!/usr/bin/env bash
# Clean baseline between phases — truncate tables, reset stats.
# Usage: ./reset.sh

set -euo pipefail

DSN="${DATABASE_URL:-postgresql://root@localhost:26257/bench?sslmode=disable}"

echo "Resetting bench database..."
cockroach sql --insecure --url "$DSN" <<SQL
  TRUNCATE TABLE orders;
  UPDATE accounts SET balance = 10000, version = 1 WHERE id IN (1, 2);
SQL

echo "Done. Tables truncated, baseline clean."
