CREATE TABLE {db}.funnel_tracking_state
(
    placement_id UInt64,
    tracking_id UInt64,
    stage LowCardinality(String),
    first_event_datetime SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    first_ingested_at SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    representative AggregateFunction(argMin,
        Tuple(UInt64, UInt64, DateTime64(3, 'UTC'), DateTime64(3, 'UTC'),
              String, String, Array(UInt64), String),
        Tuple(DateTime64(3, 'UTC'), String))
)
ENGINE = AggregatingMergeTree
ORDER BY (placement_id, tracking_id, stage);

CREATE MATERIALIZED VIEW {db}.tracking_mv TO {db}.funnel_tracking_state AS
SELECT placement_id, tracking_id, stage,
       min(event_datetime) AS first_event_datetime,
       min(ingested_at) AS first_ingested_at,
       argMinState(tuple(campaign_id, adgroup_id, event_datetime, ingested_at,
                         toString(source_type), message_id, customer_group_ids, attributes),
                   tuple(ingested_at, message_id)) AS representative
FROM {db}.funnel_event
GROUP BY placement_id, tracking_id, stage;
