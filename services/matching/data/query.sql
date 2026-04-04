/* 
* Driver
*/ 

-- name: GetDriver :one
SELECT * FROM driver
WHERE id = $1 LIMIT 1;

-- name: GetRandomAvailableDriver :one
SELECT * FROM driver
WHERE status = 'available'
ORDER BY RANDOM()
LIMIT 1;

-- name: GetNRandomAvailableDrivers :many
SELECT * FROM driver
WHERE status = 'available'
ORDER BY RANDOM()
LIMIT $1;

-- name: ListDrivers :many
SELECT * FROM driver
ORDER BY name;

-- name: CreateDriver :one
INSERT INTO driver (
    name
) VALUES (
  $1
)
RETURNING *;

-- name: DeleteDriver :exec
DELETE FROM driver
WHERE id = $1;

-- name: UpdateDriverStatus :exec
UPDATE driver
SET status = $2
WHERE id = $1
    AND status = 'available';

-- name: ResetAllDriversToAvailable :exec
UPDATE driver
SET status = 'available';

/* 
* Deduplication
*/ 
-- name: CreateDedupEntry :one
INSERT INTO deduplication (
    ride_id, stream
) VALUES (
    $1, $2
) ON CONFLICT (
    ride_id, stream
) DO NOTHING
RETURNING *;

-- name: CheckDedupEntry :one
SELECT * FROM deduplication 
WHERE ride_id = $1 
    AND stream = $2;

-- name: SetDedupProcessed :exec
UPDATE deduplication 
SET processed_at = NOW()
WHERE ride_id = $1 
    AND stream = $2;

/*
* Outbox
*/

-- name: CreateOutboxEvent :one
INSERT INTO outbox (
    ride_id, stream, payload
) VALUES (
    $1, $2, $3
) RETURNING *;

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

-- name: SetOutboxPublished :exec
UPDATE outbox 
SET published_at = NOW()
WHERE id = $1;

/* 
    reconciler Queries
*/
-- name: GetBusyDrivers :many
SELECT * FROM driver
WHERE status = 'busy';