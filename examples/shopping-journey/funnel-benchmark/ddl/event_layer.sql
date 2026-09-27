-- {db}, {stage}, {stage_lower} are rendered by benchmark.py.
CREATE TABLE {db}.{stage_lower}_event
(
    campaign_id UInt64,
    adgroup_id UInt64,
    placement_id UInt64,
    tracking_id UInt64,
    stage LowCardinality(String),
    event_datetime DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    source_type LowCardinality(String),
    message_id String,
    customer_group_ids Array(UInt64),
    attributes String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_datetime)
ORDER BY (placement_id, event_datetime, tracking_id);

CREATE MATERIALIZED VIEW {db}.route_{stage_lower}
TO {db}.{stage_lower}_event AS
SELECT * FROM {db}.ingress WHERE stage = '{stage}';
