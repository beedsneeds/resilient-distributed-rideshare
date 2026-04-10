#!/usr/bin/env bash
#
# Prereqs: ride, matching, rider and reconciler services running in separate terminals.
# Workflow per scenario:
#   0. Optional reset state
#   1. prompt you to restart the target service with FAULT_INJECT armed
#   2. check half-state in postgres + redis immediately after crash
#         Each scenario's check_halfstate captures the target rideID during the crash window
#   3. prompt you to restart the target service clean
#   4. wait for claim timeout / recovery
#   5. check_recovery asserts invariants about that specific ride after the recovery window closes.
#         Verification is done entirely through psql and redis-cli, not logs because its not working reliably

set -euo pipefail

MATCHING_DB="postgres://postgres:postgres@matching-db:5432/matching_db"
RIDE_DB="postgres://postgres:postgres@ride-db:5432/ride_db"
REDIS="-h redis"

# stuck_ride is populated by check_halfstate and consumed by check_recovery.
stuck_ride=""

# pel_for_ride <stream> <group> <ride_id>
# Counts pending entries on <stream>/<group> whose rideID field matches <ride_id>.
# Filtering by rideID avoids false failures when an unrelated ride happens to be
# sitting in the PEL from a prior scenario or a concurrent request.
pel_for_ride() {
  local stream=$1 group=$2 ride=$3 count=0 sid rid
  while read -r sid _; do
    [[ -z "$sid" ]] && continue
    rid=$(redis-cli $REDIS XRANGE "$stream" "$sid" "$sid" \
          | awk '/^rideID$/{getline; print; exit}')
    [[ "$rid" == "$ride" ]] && count=$((count+1))
  done < <(redis-cli $REDIS XPENDING "$stream" "$group" - + 100 | awk '{print $1}')
  echo "$count"
}

scenario="${1:-}"

case "$scenario" in
  matching-ghost-message)
    fault="matching.outbox.after_xadd:exit"
    target_svc="matching"
    check_halfstate() {
      stuck_ride=$(psql "$MATCHING_DB" -tAc \
        "SELECT ride_id FROM outbox WHERE stream='ride.accepted' AND retrieved_at IS NOT NULL AND published_at IS NULL ORDER BY id DESC LIMIT 1")
      [[ -z "$stuck_ride" ]] && { echo "  FAIL: no stuck matching outbox row"; return 1; }
      echo "  captured rideID: $stuck_ride"
      echo "  ride.accepted stream length: $(redis-cli $REDIS XLEN ride.accepted)"
    }
    check_recovery() {
      local published dedup pel
      published=$(psql "$MATCHING_DB" -tAc "SELECT count(*) FROM outbox WHERE ride_id='$stuck_ride' AND published_at IS NOT NULL")
      dedup=$(psql "$RIDE_DB" -tAc "SELECT count(*) FROM deduplication WHERE ride_id='$stuck_ride' AND stream='ride.accepted'")
      pel=$(pel_for_ride ride.accepted ride-group "$stuck_ride")
      echo "  stuck row published:          $published (want 1)"
      echo "  ride dedup entries:           $dedup (want 1)"
      echo "  ride-group PEL for stuck ride: $pel (want 0)"
      [[ "$published" == "1" && "$dedup" == "1" && "$pel" == "0" ]]
    }
    ;;

  ride-ghost-message)
    fault="ride.outbox.after_xadd:exit"
    target_svc="ride"
    check_halfstate() {
      stuck_ride=$(psql "$RIDE_DB" -tAc \
        "SELECT ride_id FROM outbox WHERE stream='ride.requested' AND retrieved_at IS NOT NULL AND published_at IS NULL ORDER BY id DESC LIMIT 1")
      [[ -z "$stuck_ride" ]] && { echo "  FAIL: no stuck ride outbox row"; return 1; }
      echo "  captured rideID: $stuck_ride"
      echo "  ride.requested stream length: $(redis-cli $REDIS XLEN ride.requested)"
    }
    check_recovery() {
      local published dedup pel
      published=$(psql "$RIDE_DB" -tAc "SELECT count(*) FROM outbox WHERE ride_id='$stuck_ride' AND published_at IS NOT NULL")
      dedup=$(psql "$MATCHING_DB" -tAc "SELECT count(*) FROM deduplication WHERE ride_id='$stuck_ride' AND stream='ride.requested'")
      pel=$(pel_for_ride ride.requested matching-group "$stuck_ride")
      echo "  stuck row published:              $published (want 1)"
      echo "  matching dedup:                   $dedup (want 1)"
      echo "  matching-group PEL for stuck ride: $pel (want 0)"
      [[ "$published" == "1" && "$dedup" == "1" && "$pel" == "0" ]]
    }
    ;;

  matching-claim-timeout)
    fault="matching.outbox.after_claim:exit"
    target_svc="matching"
    check_halfstate() {
      stuck_ride=$(psql "$MATCHING_DB" -tAc \
        "SELECT ride_id FROM outbox WHERE stream='ride.accepted' AND retrieved_at IS NOT NULL AND published_at IS NULL ORDER BY id DESC LIMIT 1")
      [[ -z "$stuck_ride" ]] && { echo "  FAIL: no stuck matching outbox row"; return 1; }
      echo "  captured rideID: $stuck_ride"
      echo "  ride.accepted stream length (expect 0, nothing published yet): $(redis-cli $REDIS XLEN ride.accepted)"
    }
    check_recovery() {
      local published dedup pel
      published=$(psql "$MATCHING_DB" -tAc "SELECT count(*) FROM outbox WHERE ride_id='$stuck_ride' AND published_at IS NOT NULL")
      dedup=$(psql "$RIDE_DB" -tAc "SELECT count(*) FROM deduplication WHERE ride_id='$stuck_ride' AND stream='ride.accepted'")
      pel=$(pel_for_ride ride.accepted ride-group "$stuck_ride")
      echo "  stuck row published:          $published (want 1)"
      echo "  ride dedup entries:           $dedup (want 1)"
      echo "  ride-group PEL for stuck ride: $pel (want 0)"
      [[ "$published" == "1" && "$dedup" == "1" && "$pel" == "0" ]]
    }
    ;;

  ride-claim-timeout)
    fault="ride.outbox.after_claim:exit"
    target_svc="ride"
    check_halfstate() {
      stuck_ride=$(psql "$RIDE_DB" -tAc \
        "SELECT ride_id FROM outbox WHERE stream='ride.requested' AND retrieved_at IS NOT NULL AND published_at IS NULL ORDER BY id DESC LIMIT 1")
      [[ -z "$stuck_ride" ]] && { echo "  FAIL: no stuck ride outbox row"; return 1; }
      echo "  captured rideID: $stuck_ride"
      echo "  ride.requested stream length (expect 0): $(redis-cli $REDIS XLEN ride.requested)"
    }
    check_recovery() {
      local published dedup pel
      published=$(psql "$RIDE_DB" -tAc "SELECT count(*) FROM outbox WHERE ride_id='$stuck_ride' AND published_at IS NOT NULL")
      dedup=$(psql "$MATCHING_DB" -tAc "SELECT count(*) FROM deduplication WHERE ride_id='$stuck_ride' AND stream='ride.requested'")
      pel=$(pel_for_ride ride.requested matching-group "$stuck_ride")
      echo "  stuck row published:              $published (want 1)"
      echo "  matching dedup:                   $dedup (want 1)"
      echo "  matching-group PEL for stuck ride: $pel (want 0)"
      [[ "$published" == "1" && "$dedup" == "1" && "$pel" == "0" ]]
    }
    ;;

  matching-pel-recovery)
    fault="matching.trydriver.after_commit:exit"
    target_svc="matching"
    check_halfstate() {
      stuck_ride=$(psql "$MATCHING_DB" -tAc \
        "SELECT ride_id FROM deduplication WHERE stream='ride.requested' ORDER BY id DESC LIMIT 1")
      [[ -z "$stuck_ride" ]] && { echo "  FAIL: no matching dedup entry — tx didn't commit before crash"; return 1; }
      echo "  captured rideID: $stuck_ride"
      local pel busy
      pel=$(redis-cli $REDIS XPENDING ride.requested matching-group | head -1)
      busy=$(psql "$MATCHING_DB" -tAc "SELECT count(*) FROM driver WHERE status='busy'")
      echo "  ride.requested PEL (expect >=1 unacked): $pel"
      echo "  drivers busy (expect >=1):               $busy"
    }
    check_recovery() {
      local dedup pel accepted_published
      dedup=$(psql "$MATCHING_DB" -tAc "SELECT count(*) FROM deduplication WHERE ride_id='$stuck_ride' AND stream='ride.requested'")
      pel=$(pel_for_ride ride.requested matching-group "$stuck_ride")
      accepted_published=$(psql "$MATCHING_DB" -tAc "SELECT count(*) FROM outbox WHERE ride_id='$stuck_ride' AND stream='ride.accepted' AND published_at IS NOT NULL")
      echo "  matching dedup:                    $dedup (want 1)"
      echo "  matching-group PEL for stuck ride: $pel (want 0)"
      echo "  ride.accepted published:           $accepted_published (want 1)"
      [[ "$dedup" == "1" && "$pel" == "0" && "$accepted_published" == "1" ]]
    }
    ;;

  ride-accepted-rollback)
    fault="ride.accepted.before_commit:panic"
    target_svc="ride"
    check_halfstate() {
      # Tx rolled back, so no ride-side dedup yet. Find rideID via matching's outbox.
      stuck_ride=$(psql "$MATCHING_DB" -tAc \
        "SELECT ride_id FROM outbox WHERE stream='ride.accepted' AND published_at IS NOT NULL ORDER BY id DESC LIMIT 1")
      [[ -z "$stuck_ride" ]] && { echo "  FAIL: no published ride.accepted event found"; return 1; }
      echo "  captured rideID: $stuck_ride"
      local dedup pel
      dedup=$(psql "$RIDE_DB" -tAc "SELECT count(*) FROM deduplication WHERE ride_id='$stuck_ride' AND stream='ride.accepted'")
      pel=$(redis-cli $REDIS XPENDING ride.accepted ride-group | head -1)
      echo "  ride dedup (expect 0, proves rollback): $dedup"
      echo "  ride-group PEL (expect >=1 unacked):    $pel"
    }
    check_recovery() {
      local dedup pel ride_status
      dedup=$(psql "$RIDE_DB" -tAc "SELECT count(*) FROM deduplication WHERE ride_id='$stuck_ride' AND stream='ride.accepted'")
      pel=$(pel_for_ride ride.accepted ride-group "$stuck_ride")
      ride_status=$(psql "$RIDE_DB" -tAc "SELECT ride_status FROM ride WHERE id='$stuck_ride'")
      echo "  ride dedup:                    $dedup (want 1)"
      echo "  ride-group PEL for stuck ride: $pel (want 0)"
      echo "  ride status:                   $ride_status (want accepted)"
      [[ "$dedup" == "1" && "$pel" == "0" && "$ride_status" == "accepted" ]]
    }
    ;;

  ride-request-retry)
    fault="ride.request.after_commit:exit"
    target_svc="ride"
    check_halfstate() {
      stuck_ride=$(psql "$RIDE_DB" -tAc "SELECT id FROM ride ORDER BY requested_at DESC LIMIT 1")
      [[ -z "$stuck_ride" ]] && { echo "  FAIL: no ride row found"; return 1; }
      echo "  captured rideID: $stuck_ride"
      local count
      count=$(psql "$RIDE_DB" -tAc "SELECT count(*) FROM ride")
      echo "  ride count (expect 1, from the pre-crash commit): $count"
    }
    check_recovery() {
      local count
      count=$(psql "$RIDE_DB" -tAc "SELECT count(*) FROM ride")
      echo "  ride count: $count (want 1)"
      [[ "$count" == "1" ]]
    }
    ;;

  *)
    echo "usage: $0 <scenario>" >&2
    echo >&2
    echo "scenarios:" >&2
    echo "  matching-ghost-message    matching crashes after XAdd on ride.accepted" >&2
    echo "  ride-ghost-message        ride crashes after XAdd on ride.requested" >&2
    echo "  matching-claim-timeout    matching crashes after claim, before XAdd" >&2
    echo "  ride-claim-timeout        ride crashes after claim, before XAdd" >&2
    echo "  matching-pel-recovery     matching crashes after tryDriver commit" >&2
    echo "  ride-accepted-rollback    ride panics inside accepted-handler tx" >&2
    echo "  ride-request-retry        ride crashes after RequestRide commit; verifies idempotency" >&2
    exit 1
    ;;
esac

echo
echo "=== scenario: $scenario ==="
echo "fault:  $fault"
echo "target: $target_svc service"
echo

echo
echo "STEP 1: In the $target_svc terminal, Ctrl-C and run:"
echo
echo "    FAULT_INJECT=\"$fault\" go run ."
echo
read -rp "Press Enter once the service has crashed... "

echo
echo "STEP 2: Capturing half-state..."
if ! check_halfstate; then
  echo
  echo "half-state check failed — aborting. The fault may not have fired"
  exit 1
fi

echo
echo "STEP 3: In the $target_svc terminal, restart cleanly:"
echo
echo "    go run ."
echo
read -rp "Press Enter once it's running again... "

echo
echo "STEP 4: Waiting 30 seconds for recovery..."
sleep 30

echo
echo "STEP 5: Verifying recovery state..."
if check_recovery; then
  echo
  echo "PASS"
  echo "Optional: Reset state with ./faultinject/resetstate.sh"
  exit 0
else
  echo
  echo "FAIL"
  echo "Optional: Reset state with ./faultinject/resetstate.sh"
  exit 1
fi

