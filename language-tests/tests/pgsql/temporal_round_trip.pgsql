/**
[env]
namespace = true
database = true
imports = ["pgsql/_temporal_columns.surql"]

[test]
reason = "date, time, timestamp and interval values read back as their own kinds, their text form is the PostgreSQL one, a value written through INSERT is read back the same, and comparisons promote date to timestamp to timestamptz and read a quoted literal as the column's type"

[[test.results]]
value = "[{ d: pd'2026-10-27', iv: ivl'P1Y2M3DT4H5M6.5S', t: pt'12:30:00.500', ts: ts'2026-10-27T12:30:00.500' }, { d: pd'1999-12-31', iv: ivl'P-1DT-2H', t: pt'24:00:00', ts: ts'1999-12-31T23:59:59' }]"

[[test.results]]
value = "[{ d: pd'2030-01-02', id: 3, iv: ivl'P2M-3D', t: pt'01:02:03.000004', ts: ts'2030-01-02T01:02:03.000004' }]"

[[test.results]]
value = "[{ d: pd'2030-01-02', iv: ivl'P2M-3D', t: pt'01:02:03.000004', ts: ts'2030-01-02T01:02:03.000004' }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }]"

[[test.results]]
value = "[{ id: 2 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }]"

[[test.results]]
value = "[{ id: 1, next_day: pd'2026-10-28', tomorrow: ts'2026-10-28T12:30:00.500' }, { id: 2, next_day: pd'2000-01-01', tomorrow: ts'2000-01-01T23:59:59' }, { id: 3, next_day: pd'2030-01-03', tomorrow: ts'2030-01-03T01:02:03.000004' }]"

*/

SELECT d, t, ts, iv FROM temporal_trip ORDER BY id;

INSERT INTO temporal_trip (id, d, t, ts, iv) VALUES (3, CAST('2030-01-02' AS date), CAST('01:02:03.000004' AS time), CAST('2030-01-02 01:02:03.000004' AS timestamp), CAST('2 mons -3 days' AS interval));

SELECT d, t, ts, iv FROM temporal_trip WHERE id = 3;

SELECT id FROM temporal_trip WHERE d = '2026-10-27' AND ts > '2026-10-27 12:00:00' ORDER BY id;

SELECT id FROM temporal_trip WHERE iv = CAST('14 months 3 days 04:05:06.5' AS interval) ORDER BY id;

SELECT id FROM temporal_trip WHERE d > CAST('2000-01-01 00:00:00' AS timestamp) ORDER BY id;

SELECT id FROM temporal_trip WHERE d < CAST('2000-01-01 00:00:00+00' AS timestamptz) ORDER BY id;

SELECT id FROM temporal_trip WHERE ts - CAST('2026-10-27 12:00:00' AS timestamp) > CAST('30 minutes' AS interval) ORDER BY id;

SELECT id, d + 1 AS next_day, ts + CAST('1 day' AS interval) AS tomorrow FROM temporal_trip ORDER BY id;
