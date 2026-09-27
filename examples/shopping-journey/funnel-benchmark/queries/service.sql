-- Example: render {db} and choose matching aggregate function.
-- Half-open aligned range. States are merged over the WHOLE requested period.
SELECT placement_id,
    uniqCombined64MergeIf(tracking_state, stage = 'LANDING') AS landing,
    uniqCombined64MergeIf(tracking_state, stage = 'OPEN') AS open,
    uniqCombined64MergeIf(tracking_state, stage = 'CLICK') AS click,
    uniqCombined64MergeIf(tracking_state, stage = 'CONVERSION') AS conversion
FROM {db}.funnel_metric_1h
WHERE placement_id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
  AND metric_time >= toDateTime('2026-08-01 00:00:00', 'UTC')
  AND metric_time < toDateTime('2026-09-01 00:00:00', 'UTC')
GROUP BY placement_id;
-- Arbitrary second boundaries: see hybrid_query() in benchmark.py.
-- Never sum finalized minute/hour distinct counts.
