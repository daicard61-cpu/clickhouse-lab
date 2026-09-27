CREATE TABLE {db}.funnel_metric_1h AS {db}.funnel_metric_1m
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(metric_time)
ORDER BY (placement_id, metric_time, stage, campaign_id, adgroup_id);

CREATE MATERIALIZED VIEW {db}.rollup_1h TO {db}.funnel_metric_1h AS
SELECT campaign_id, adgroup_id, placement_id,
       toStartOfHour(metric_time) AS metric_time, stage,
       {function}MergeState(tracking_state) AS tracking_state
FROM {db}.funnel_metric_1m
GROUP BY campaign_id, adgroup_id, placement_id, metric_time, stage;
