CREATE TYPE ridestatus AS ENUM (
    'unspecified',
    'requested',
    -- 'matching',
    -- 'matched',
    'accepted'
    -- 'in_progress',
    -- 'completed',
    -- 'cancelled',
    -- 'failed'
);

CREATE TYPE stream AS ENUM (
    'ride.requested',
    'ride.accepted'
);


CREATE TABLE ride(
    id          UUID   PRIMARY KEY DEFAULT gen_random_uuid(),
    -- the above default is a fallback. I want to generate from the server alongside idempotency key
    -- ofc no idempotency required for rider/driver
    rider_id    UUID    NOT NULL,
    driver_id   UUID,
    ride_status ridestatus  NOT NULL    DEFAULT 'requested',

    requested_at    TIMESTAMP   DEFAULT NOW(),
    accepted_at     TIMESTAMP
);

CREATE TABLE deduplication (
    id              UUID                PRIMARY KEY DEFAULT gen_random_uuid(),
    ride_id         UUID NOT NULL,                
    stream          stream NOT NULL,
    processed_at    TIMESTAMP,
    UNIQUE (ride_id, stream) -- composite key that identifies a unique ride having only one unique operation
);

CREATE TABLE requestDedup (
    idempKey        UUID                PRIMARY KEY DEFAULT gen_random_uuid(),
    ride_id         UUID
);

CREATE TABLE outbox (
    id          UUID   PRIMARY KEY DEFAULT gen_random_uuid(), 
    ride_id          UUID NOT NULL, -- not using ride_id as pk because I'll add other event types, so ride_id won't be unique
    stream          stream NOT NULL DEFAULT 'ride.requested', 
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    retrieved_at    TIMESTAMP,
    -- is there a postgres query pattern that updates something when you fetch it?
    -- should I have a 'retrieved_at' field that determines whether I should attempt republishing stale outbox 
    -- entries that don't have a published_at BUT could potentially be processed?
    -- I know an outbox does not guarantee exactly once semantics. I use a deduplication table for this
    published_at    TIMESTAMP
);
