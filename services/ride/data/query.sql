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
WHERE id = $1;

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

-- name: UpdateRideMatching :one
UPDATE ride
SET ride_status = 'matching',
    matching_at = NOW()
WHERE id = $1
    AND ride_status = 'requested'
RETURNING *;

-- name: UpdateRideMatched :one
UPDATE ride
SET ride_status = 'matched',
    matched_at = NOW()
WHERE id = $1
    AND ride_status = 'matching'
RETURNING *;

-- name: UpdateRideAccepted :one
UPDATE ride
SET driver_id = $2,
    ride_status = 'accepted',
    accepted_at = NOW()
WHERE id = $1
    AND ride_status = 'matched'
RETURNING *;


-- name: DeleteRide :exec
DELETE FROM ride
WHERE id = $1;









