#!/usr/bin/env bash
# Wipes all test state. Safe to run anytime.
#
# Clears Redis (streams, consumer groups, PEL, locks), truncates dedup +
# outbox tables in both service DBs, resets drivers to available, then
# recreates the consumer groups so the still-running services can keep
# consuming without needing a restart
set -euo pipefail

echo "flushing redis..."
redis-cli -h redis FLUSHDB > /dev/null

echo "recreating consumer groups..."
redis-cli -h redis XGROUP CREATE ride.requested matching-group '$' MKSTREAM > /dev/null
redis-cli -h redis XGROUP CREATE ride.accepted ride-group '$' MKSTREAM > /dev/null

echo "clearing matching db..."
psql postgres://postgres:postgres@matching-db:5432/matching_db <<'SQL' > /dev/null
TRUNCATE outbox, deduplication RESTART IDENTITY;
UPDATE driver SET status = 'available';
SQL

echo "clearing ride db..."
psql postgres://postgres:postgres@ride-db:5432/ride_db <<'SQL' > /dev/null
TRUNCATE outbox, deduplication, ride, requestDedup RESTART IDENTITY CASCADE;
SQL

echo "reset complete."