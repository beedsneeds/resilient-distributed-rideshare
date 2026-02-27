CREATE TYPE riderstatus AS ENUM(
    'available',
    'on a ride'
);

CREATE TABLE rider (
    id      UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    name    VARCHAR(50)     NOT NULL,
    status  riderstatus    NOT NULL    DEFAULT 'available'

);
