#!/usr/bin/env bash
# Wipes all test state. DO NOT RUN when services are alive to avoid races.
#
# Truncates dedup + outbox tables in both service DBs and resets drivers
# to available, then flushes Redis and recreates consumer groups.

set -euo pipefail

echo "clearing matching db..."
psql postgres://postgres:postgres@matching-db:5432/matching_db <<'SQL' > /dev/null
TRUNCATE outbox, deduplication RESTART IDENTITY;
UPDATE driver SET status = 'available';
SQL

echo "clearing ride db..."
psql postgres://postgres:postgres@ride-db:5432/ride_db <<'SQL' > /dev/null
TRUNCATE outbox, deduplication, ride, requestDedup RESTART IDENTITY CASCADE;
SQL

echo "flushing redis..."
redis-cli -h redis FLUSHDB > /dev/null

echo "reset complete."