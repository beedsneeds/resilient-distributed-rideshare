CREATE TYPE ridestatus AS ENUM (
    'unspecified',
    'requested',
    'matching',
    'matched',
    'accepted',
    'in_progress',
    'completed',
    'cancelled',
    'failed'
);


CREATE TABLE ride(
    id          UUID   PRIMARY KEY DEFAULT gen_random_uuid(),
    -- the above default is a fallback. I want to generate from the server alongside idempotency key
    -- ofc no idempotency required for rider/driver
    rider_id    UUID    NOT NULL, -- should I add a default here?
    driver_id   UUID,
    ride_status ridestatus  NOT NULL    DEFAULT 'requested',

    requested_at    TIMESTAMP   DEFAULT NOW(),
    matching_at     TIMESTAMP,
    matched_at      TIMESTAMP,
    accepted_at     TIMESTAMP
);
