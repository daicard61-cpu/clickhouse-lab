-- stage dimension avoids four mostly-empty state columns.
CREATE TABLE {db}.funnel_metric_1m
(
    campaign_id UInt64,
    adgroup_id UInt64,
    placement_id UInt64,
    metric_time DateTime('UTC'),
    stage LowCardinality(String),
    tracking_state AggregateFunction({function}, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(metric_time)
ORDER BY (placement_id, metric_time, stage, campaign_id, adgroup_id);
