#!/usr/bin/env bash
# faultinjection/test.sh — drive a fault scenario and verify recovery.
#
# Prereqs: ride, matching, and rider services running in separate terminals
# inside the devcontainer, each piping output to /tmp/<svc>-service.log via tee.
#
# Workflow per scenario:
#   0. reset state (calls reset.sh)
#   1. prompt you to restart the target service with FAULT_INJECT armed
#   2. check half-state in postgres + redis immediately after crash
#   3. prompt you to restart the target service clean
#   4. wait for claim timeout / recovery
#   5. grep logs for the expected recovery line, then run optional post-recovery checks
set -euo pipefail

scenario="${1:-}"

case "$scenario" in
  matching-ghost-message)
    fault="matching.outbox.after_xadd:exit"
    target_svc="matching"
    check_halfstate() {
      echo "  matching outbox (expect >=1 claimed-but-unpublished):"
      psql postgres://postgres:postgres@matching-db:5432/matching_db -tAc \
        "SELECT count(*) FROM outbox WHERE retrieved_at IS NOT NULL AND published_at IS NULL;" \
        | xargs -I{} echo "    unpublished-but-claimed: {}"
      echo "  ride.accepted stream (expect >=1, the ghost):"
      redis-cli -h redis XLEN ride.accepted | xargs -I{} echo "    length: {}"
    }
    expect_log="ride may have already matched"
    expect_in="ride"
    ;;

  ride-ghost-message)
    fault="ride.outbox.after_xadd:exit"
    target_svc="ride"
    check_halfstate() {
      echo "  ride outbox (expect >=1 claimed-but-unpublished):"
      psql postgres://postgres:postgres@ride-db:5432/ride_db -tAc \
        "SELECT count(*) FROM outbox WHERE retrieved_at IS NOT NULL AND published_at IS NULL;" \
        | xargs -I{} echo "    unpublished-but-claimed: {}"
      echo "  ride.requested stream (expect >=1, the ghost):"
      redis-cli -h redis XLEN ride.requested | xargs -I{} echo "    length: {}"
    }
    expect_log="duplicate ride"
    expect_in="matching"
    ;;

  matching-claim-timeout)
    fault="matching.outbox.after_claim:exit"
    target_svc="matching"
    check_halfstate() {
      echo "  matching outbox (expect >=1 claimed-but-unpublished):"
      psql postgres://postgres:postgres@matching-db:5432/matching_db -tAc \
        "SELECT count(*) FROM outbox WHERE retrieved_at IS NOT NULL AND published_at IS NULL;" \
        | xargs -I{} echo "    unpublished-but-claimed: {}"
      echo "  ride.accepted stream (expect 0, nothing published yet):"
      redis-cli -h redis XLEN ride.accepted | xargs -I{} echo "    length: {}"
    }
    # After claim timeout, matching reclaims and publishes. Ride then consumes normally.
    expect_log="Successfully processed"
    expect_in="ride"
    ;;

  ride-claim-timeout)
    fault="ride.outbox.after_claim:exit"
    target_svc="ride"
    check_halfstate() {
      echo "  ride outbox (expect >=1 claimed-but-unpublished):"
      psql postgres://postgres:postgres@ride-db:5432/ride_db -tAc \
        "SELECT count(*) FROM outbox WHERE retrieved_at IS NOT NULL AND published_at IS NULL;" \
        | xargs -I{} echo "    unpublished-but-claimed: {}"
      echo "  ride.requested stream (expect 0, nothing published yet):"
      redis-cli -h redis XLEN ride.requested | xargs -I{} echo "    length: {}"
    }
    expect_log="Driver .* accepted ride"
    expect_in="matching"
    ;;

  matching-pel-recovery)
    fault="matching.trydriver.after_commit:exit"
    target_svc="matching"
    check_halfstate() {
      echo "  matching dedup (expect >=1, proves tx committed before crash):"
      psql postgres://postgres:postgres@matching-db:5432/matching_db -tAc \
        "SELECT count(*) FROM deduplication WHERE stream = 'ride.requested';" \
        | xargs -I{} echo "    entries: {}"
      echo "  matching drivers busy (expect >=1):"
      psql postgres://postgres:postgres@matching-db:5432/matching_db -tAc \
        "SELECT count(*) FROM driver WHERE status = 'busy';" \
        | xargs -I{} echo "    busy: {}"
      echo "  ride.requested PEL (expect >=1 unacked):"
      redis-cli -h redis XPENDING ride.requested matching-group \
        | head -1 | xargs -I{} echo "    {}"
    }
    expect_log="duplicate ride"
    expect_in="matching"
    ;;

  ride-accepted-rollback)
    fault="ride.accepted.before_commit:panic"
    target_svc="ride"
    check_halfstate() {
      echo "  ride dedup (expect 0, proves rollback worked):"
      psql postgres://postgres:postgres@ride-db:5432/ride_db -tAc \
        "SELECT count(*) FROM deduplication WHERE stream = 'ride.accepted';" \
        | xargs -I{} echo "    entries: {}"
      echo "  ride.accepted PEL (expect >=1 unacked):"
      redis-cli -h redis XPENDING ride.accepted ride-group \
        | head -1 | xargs -I{} echo "    {}"
    }
    expect_log="Successfully processed"
    expect_in="ride"
    ;;

  ride-request-retry)
    fault="ride.request.after_commit:exit"
    target_svc="ride"
    check_halfstate() {
      echo "  rides table (expect exactly 1, from the pre-crash commit):"
      psql postgres://postgres:postgres@ride-db:5432/ride_db -tAc \
        "SELECT count(*) FROM ride;" \
        | xargs -I{} echo "    count: {}"
    }
    check_postrecovery() {
      local count
      count=$(psql postgres://postgres:postgres@ride-db:5432/ride_db -tAc "SELECT count(*) FROM ride;")
      if [[ "$count" == "1" ]]; then
        echo "  PASS: exactly 1 ride after retry (idempotency working)"
        return 0
      else
        echo "  FAIL: expected 1 ride, got $count (duplicate created on retry)"
        return 1
      fi
    }
    # NOTE: update this to whatever line your RequestRide logs on idempotent replay.
    expect_log="ride already requested with ID"
    expect_in="ride"
    ;;

  *)
    echo "usage: $0 <scenario>" >&2
    echo >&2
    echo "scenarios:" >&2
    echo "  matching-ghost-message          matching crashes after XAdd on ride.accepted" >&2
    echo "  ride-ghost-message     ride crashes after XAdd on ride.requested" >&2
    echo "  matching-claim-timeout matching crashes after claim, before XAdd" >&2
    echo "  ride-claim-timeout        ride crashes after claim, before XAdd" >&2
    echo "  matching-pel-recovery   matching crashes after tryDriver commit" >&2
    echo "  ride-accepted-rollback      ride panics inside accepted-handler tx" >&2
    echo "  ride-request-retry        ride crashes after RequestRide commit; verifies idempotency" >&2
    exit 1
    ;;
esac

echo
echo "=== scenario: $scenario ==="
echo "fault:  $fault"
echo "target: $target_svc service"
echo

echo "STEP 0: Resetting state..."
"$(dirname "$0")/resetstate.sh"

echo
echo "STEP 1: In the $target_svc service terminal, Ctrl-C the current process"
echo "and run:"
echo
echo "    FAULT_INJECT=\"$fault\" go run . 2>&1 | tee -a /tmp/${target_svc}-service.log"
echo
read -rp "Press Enter once the service has crashed with 'os.Exit(1)' or a panic... "

echo
echo "STEP 2: Checking half-state before recovery..."
check_halfstate

echo
echo "STEP 3: In the $target_svc terminal, restart it cleanly:"
echo
echo "    go run . 2>&1 | tee -a /tmp/${target_svc}-service.log"
echo
read -rp "Press Enter once it's running again... "

echo
echo "STEP 4: Waiting 30 seconds for recovery (claim timeout is 15s)..."
sleep 30

echo
echo "STEP 5: Verifying recovery..."
log_file="/tmp/${expect_in}-service.log"
if [[ ! -f "$log_file" ]]; then
  echo "  SKIP: $log_file not found."
  echo "  Make sure you started the $expect_in service with:"
  echo "    go run . 2>&1 | tee /tmp/${expect_in}-service.log"
  exit 1
fi

log_pass=0
if tail -500 "$log_file" | grep -qE "$expect_log"; then
  echo "  PASS: found '$expect_log' in $expect_in service log"
  log_pass=1
else
  echo "  FAIL: did not find '$expect_log' in $expect_in service log"
  echo "  last 20 lines of $log_file:"
  tail -20 "$log_file" | sed 's/^/    /'
fi

# Optional per-scenario post-recovery invariant checks.
postrecovery_pass=1
if declare -f check_postrecovery > /dev/null; then
  echo
  echo "  post-recovery checks:"
  if ! check_postrecovery; then
    postrecovery_pass=0
  fi
fi

if [[ $log_pass -eq 1 && $postrecovery_pass -eq 1 ]]; then
  exit 0
else
  exit 1
fi