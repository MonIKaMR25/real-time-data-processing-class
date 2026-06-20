#!/usr/bin/env bash
# Source this file to load lesson defaults into your shell:
#   source ./use-lesson-env.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a
source "$SCRIPT_DIR/.env"
set +a
echo "DATABASE_URL=$DATABASE_URL"
