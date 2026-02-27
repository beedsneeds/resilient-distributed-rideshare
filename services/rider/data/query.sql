/* 
* Rider
*/ 

-- name: GetRider :one
SELECT * FROM rider
WHERE id = $1 LIMIT 1;

-- name: GetRandomAvailableRider :one
SELECT * FROM rider
WHERE status = 'available'
ORDER BY RANDOM()
LIMIT 1;

-- name: ListRiders :many
SELECT * FROM rider
ORDER BY name;

-- name: DeleteRider :exec
DELETE FROM rider 
WHERE id = $1;

-- name: CreateRider :one
INSERT INTO rider (
    name
) VALUES (
  $1
)
RETURNING *;

-- name: UpdateRiderStatus :exec
UPDATE rider
SET status = $2
WHERE id = $1;




