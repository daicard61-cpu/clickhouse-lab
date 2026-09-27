CREATE TABLE {db}.funnel_tracking_state
(
    placement_id UInt64,
    tracking_id UInt64,
    stage LowCardinality(String),
    first_event_datetime SimpleAggregateFunction(min, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY (placement_id, tracking_id, stage);
