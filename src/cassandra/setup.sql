CREATE KEYSPACE events
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE events;

CREATE TABLE user_actions (
    event_timestamp INT,
    event_id INT,
    event_type TEXT,
    device_type TEXT,
    user_id TEXT,
    PRIMARY KEY ((user_id), event_timestamp)
)
WITH CLUSTERING ORDER BY (event_timestamp DESC);