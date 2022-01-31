CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS event (
    event_id uuid DEFAULT uuid_generate_v1 (),
    event VARCHAR NOT NULL,
    written_at BIGINT,
    PRIMARY KEY (event_id)
);

INSERT INTO event (event, written_at) VALUES ('{"action": "upsert", "id": 0, "value": "value"}', 0);