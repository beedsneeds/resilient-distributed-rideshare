/* 
* Ride
*/ 

-- name: GetRide :one
SELECT * FROM ride
WHERE id = $1 LIMIT 1;

-- name: ListRides :many
SELECT * FROM ride
ORDER BY requested_at;

-- name: CreateRide :one
INSERT INTO ride (
  id, rider_id
) VALUES (
  $1, $2
)
RETURNING *;

-- name: UpdateRideStatus :one
UPDATE ride
SET ride_status = $2
WHERE id = $1 
    AND $2::ridestatus IN ('completed', 'cancelled', 'failed', 'in_progress')
RETURNING *;
-- TODO: check if 1) ride exists 2) the correct enum is being inserted

/* 
* Don't condense into a single update to protect valid state transitions 
*/
-- name: SetRideMatching :one
UPDATE ride
SET ride_status = 'matching',
    matching_at = NOW()
WHERE id = $1
    AND ride_status = 'requested'
RETURNING *;

/*
* We aren't using this because we assume the human element in the transition of matched -> accepted 
* does not exist and that it will always succeed. Will extend functionality later (maybe)
*/
-- name: SetRideMatched :one
UPDATE ride
SET ride_status = 'matched',
    matched_at = NOW()
WHERE id = $1
    AND ride_status = 'matching'
RETURNING *;

-- name: SetRideAccepted :one
UPDATE ride
SET driver_id = $2,
    ride_status = 'accepted',
    accepted_at = NOW()
WHERE id = $1
    AND ride_status = 'matching'
RETURNING *;


-- name: DeleteRide :exec
DELETE FROM ride
WHERE id = $1;

/* 
* Deduplication and Outbox
*/ 
-- name: CreateOutboxEvent :one
INSERT INTO outbox (
  ride_id, stream
) VALUES (
  $1, $2
) 
RETURNING *;

-- name: ClaimOutboxEvent :one
UPDATE outbox
SET retrieved_at = NOW()
WHERE id = (
    SELECT id
    FROM outbox
    WHERE published_at IS NULL
    AND (
      retrieved_at IS NULL  -- fresh rows
      OR retrieved_at < NOW() - INTERVAL '1 second' * $1 -- stale / timed out rows
    )
    ORDER BY created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;

-- name: SetOutboxPublished :one
UPDATE outbox 
SET published_at = NOW()
WHERE id = $1
RETURNING *;

/* 
    Reconciliation Queries
*/
-- name: ListStaleRides :many
SELECT * FROM ride
WHERE ride_status = $1
  AND requested_at < NOW() - INTERVAL '1 second' * $2;

/* 
* We assume rider_id don't make multiple requests within duplicateRideThreshold seconds
*/
-- -- name: GetDuplicatedRides :many
-- -- PROBLEM WITH HAVING
-- SELECT rider_id, count(*)  FROM ride
--   WHERE requested_at < NOW() - INTERVAL '1 second' * $1
-- ORDER BY rider_id 
-- HAVING COUNT(*) > 1;
-- SELECT id, rider_id, ride_status, requested_at FROM ride
--   WHERE requested_at > NOW() - INTERVAL '1 second' * $1
--     AND ride_status NOT IN ('cancelled', 'failed', 'completed')
--   ORDER BY rider_id, requested_at ASC;

-- SELECT id, rider_id, ride_status, requested_at FROM (
--   SELECT *, COUNT(*) OVER (PARTITION BY rider_id) AS cnt
--   FROM ride
--   WHERE requested_at > NOW() - INTERVAL '1 second' * $1
--     AND ride_status NOT IN ('cancelled', 'failed', 'completed')
-- ) subquery
-- WHERE cnt > 1
-- ORDER BY rider_id, requested_at ASC;









