-- Fixed reference time, IDs, no production identifiers or payloads.
-- Each (placement, tracking, stage) has four occurrences across minutes/days.
-- Repetitions occupy separate input passes, not merely the same INSERT block.
INSERT INTO {db}.raw
WITH
    toUInt64(intDiv(number, 4) % {tracking_count}) AS t,
    toUInt64(intDiv(number, 4 * {tracking_count})) AS repetition,
    toUInt64(if(t % 5 = 0, 1, 2 + intHash64(t) % 1999)) AS p,
    toDateTime64('2026-09-01 00:00:00', 3, 'UTC') - INTERVAL 90 DAY
        + toIntervalSecond(intHash64(t + 17) % (89 * 86400)) AS base_time,
    base_time + toIntervalSecond([0, 60, 1800, 86400][repetition + 1]) AS ev_time
SELECT
    number AS seq,
    toUInt64(1 + intDiv(p - 1, 100)) AS campaign_id,
    toUInt64(1 + intDiv(p - 1, 10)) AS adgroup_id,
    p AS placement_id,
    t + 1 AS tracking_id,
    ['LANDING', 'OPEN', 'CLICK', 'CONVERSION'][number % 4 + 1] AS stage,
    ev_time AS event_datetime,
    ev_time + toIntervalSecond(if(repetition = 0, 172800, 1)) AS ingested_at,
    if(t % 2 = 0, 'web', 'app') AS source_type,
    concat('synthetic-', toString(number)) AS message_id,
    [toUInt64(t % 20), toUInt64(t % 5 + 100)] AS customer_group_ids,
    concat(hex(cityHash64(t)), repeat('synthetic-attribute-', 6)) AS attributes
FROM numbers({offset}, {count});
