/**
[env]
namespace = true
database = true

[test]
reason = "CAST to date, time, timestamp and interval yields those kinds, and the PostgreSQL arithmetic and promotions over them hold"

[[test.results]]
value = "[{ d: pd'2026-10-27', iv: ivl'P1Y2M', t: pt'12:30:00', ts: ts'2026-10-27T00:00:00' }]"

[[test.results]]
value = "[{ d: 'date', t: 'time without time zone', ts: 'timestamp without time zone' }]"

[[test.results]]
value = "[{ ttz: '08:00:00' }]"

[[test.results]]
value = "[{ diff: 3, minus: pd'2026-10-24', plus: pd'2026-10-30' }]"

[[test.results]]
value = "[{ clamp: ts'2026-02-28T10:00:00', d_plus: ts'2026-11-27T00:00:00', gap: ivl'P1DT1H30M' }]"

[[test.results]]
value = "[{ combined: ts'2026-10-27T12:30:00', span: ivl'PT1H30M', wrap: pt'01:00:00' }]"

[[test.results]]
value = "[{ doubled: ivl'P2D', equal: true, halved: ivl'P1DT12H', less: true }]"

[[test.results]]
value = "[{ date_eq_timestamp: true, date_lt_timestamptz: false, timestamp_gt_timestamptz: true }]"

[[test.results]]
value = "[{ date_kind: 'date', same_day: true, time_kind: 'time without time zone', today: true }]"

*/

SELECT CAST('2026-10-27 12:30:00' AS date) AS d, CAST('2026-10-27 12:30:00' AS time) AS t, CAST('2026-10-27' AS timestamp) AS ts, CAST('1 year 2 mons' AS interval) AS iv;

SELECT pg_typeof(CAST(CAST('2026-10-27T12:30:00Z' AS timestamptz) AS date)) AS d, pg_typeof(CAST(CAST('2026-10-27T12:30:00Z' AS timestamptz) AS time)) AS t, pg_typeof(CAST(CAST('2026-10-27T12:30:00Z' AS timestamptz) AS timestamp)) AS ts;

SELECT CAST(CAST('10:00:00+02' AS timetz) AS text) AS ttz;

SELECT CAST('2026-10-27' AS date) + 3 AS plus, CAST('2026-10-27' AS date) - 3 AS minus, CAST('2026-10-30' AS date) - CAST('2026-10-27' AS date) AS diff;

SELECT CAST('2026-10-27' AS date) + CAST('1 month' AS interval) AS d_plus, CAST('2026-01-31 10:00:00' AS timestamp) + CAST('1 month' AS interval) AS clamp, CAST('2026-10-27 12:00:00' AS timestamp) - CAST('2026-10-26 10:30:00' AS timestamp) AS gap;

SELECT CAST('10:00:00' AS time) + CAST('15 hours' AS interval) AS wrap, CAST('12:00:00' AS time) - CAST('10:30:00' AS time) AS span, CAST('2026-10-27' AS date) + CAST('12:30:00' AS time) AS combined;

SELECT CAST('1 mon' AS interval) = CAST('30 days' AS interval) AS equal, CAST('1 day' AS interval) < CAST('25 hours' AS interval) AS less, CAST('1 day' AS interval) * 2 AS doubled, CAST('3 days' AS interval) / 2 AS halved;

SELECT CAST('2026-10-27' AS date) = CAST('2026-10-27 00:00:00' AS timestamp) AS date_eq_timestamp, CAST('2026-10-27' AS date) < CAST('2026-10-27 00:00:00+00' AS timestamptz) AS date_lt_timestamptz, CAST('2026-10-27 01:00:00' AS timestamp) > CAST('2026-10-27 00:00:00+00' AS timestamptz) AS timestamp_gt_timestamptz;

SELECT current_date <= now() AS today, pg_typeof(current_date) AS date_kind, pg_typeof(current_time) AS time_kind, current_date = CAST(now() AS date) AS same_day;
