CREATE TABLE IF NOT EXISTS events (
    aggregate_id bigint,
    data VARCHAR,
    version integer,
    event_sequence_id BIGSERIAL
);

CREATE INDEX events_low_event_sequence_id ON events (event_sequence_id ASC);

CREATE TABLE IF NOT EXISTS aggregates (
    aggregate_id bigint,
    name VARCHAR,
    type VARCHAR,
    version integer
)