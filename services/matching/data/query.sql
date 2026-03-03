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
