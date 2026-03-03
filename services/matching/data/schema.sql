CREATE TYPE driverstatus AS ENUM(
    'available',
    'offline',
    'busy'
);


CREATE TABLE driver (
    id      UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    name    VARCHAR(50)     NOT NULL,
    status  driverstatus    NOT NULL    DEFAULT 'available'
);