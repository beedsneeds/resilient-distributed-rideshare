CREATE TYPE driverstatus AS ENUM(
    'available',
    'offline',
    'busy'
);

CREATE TYPE stream AS ENUM (
    'ride.requested',
    'ride.accepted'
);


CREATE TABLE driver (
    id      UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    name    VARCHAR(50)     NOT NULL,
    status  driverstatus    NOT NULL    DEFAULT 'available'
);

CREATE TABLE deduplication (
    id              UUID                PRIMARY KEY DEFAULT gen_random_uuid(),
    ride_id         UUID NOT NULL,                
    stream          stream NOT NULL,
    processed_at    TIMESTAMP,
    UNIQUE (ride_id, stream)
);

CREATE TABLE outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ride_id         UUID NOT NULL,
    stream          stream NOT NULL,
    payload         JSONB,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    retrieved_at    TIMESTAMP,
    published_at    TIMESTAMP
);
