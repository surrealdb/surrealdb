/**
[env]
namespace = true
database = true
imports = ["pgsql/_temporal_infinity.surql"]

[test]
reason = "infinity and -infinity are the greatest and least date, timestamp and timestamptz values: they are written and read back in their columns, order after and before every finite value, isfinite tells them apart, an interval leaves them unchanged, and the difference of an infinite timestamp is an error"

[[test.results]]
value = "[{ d: pd'infinity', id: 1, ts: ts'infinity', tz: d'+262142-12-31T23:59:59.999999999Z' }, { d: pd'-infinity', id: 2, ts: ts'-infinity', tz: d'-262143-01-01T00:00:00Z' }, { d: pd'2026-10-27', id: 3, ts: ts'2026-10-27T12:00:00', tz: d'2026-10-27T12:00:00Z' }]"

[[test.results]]
value = "[{ d: pd'infinity', id: 1, ts: ts'infinity' }, { d: pd'-infinity', id: 2, ts: ts'-infinity' }, { d: pd'2026-10-27', id: 3, ts: ts'2026-10-27T12:00:00' }]"

[[test.results]]
value = "[{ id: 2 }]"

[[test.results]]
value = "[{ id: 2 }, { id: 3 }, { id: 1 }]"

[[test.results]]
value = "[{ d_finite: false, id: 1, ts_finite: false, tz_finite: false }, { d_finite: false, id: 2, ts_finite: false, tz_finite: false }, { d_finite: true, id: 3, ts_finite: true, tz_finite: true }]"

[[test.results]]
value = "[{ a: pd'infinity', b: ts'-infinity', c: ts'infinity' }]"

[[test.results]]
value = "[{ after: true, before: true }]"

[[test.results]]
value = "[{ e: pd'1970-01-01', t: ts'1970-01-01T00:00:00' }]"

*/

INSERT INTO inf_event (id, d, ts, tz) VALUES (1, CAST('infinity' AS date), CAST('infinity' AS timestamp), CAST('infinity' AS timestamptz)), (2, CAST('-infinity' AS date), CAST('-infinity' AS timestamp), CAST('-infinity' AS timestamptz)), (3, CAST('2026-10-27' AS date), CAST('2026-10-27 12:00:00' AS timestamp), CAST('2026-10-27 12:00:00+00' AS timestamptz));

SELECT id, d, ts FROM inf_event ORDER BY id;

SELECT id FROM inf_event WHERE d < CAST('2026-10-27' AS date) ORDER BY id;

SELECT id FROM inf_event ORDER BY d;

SELECT id, isfinite(d) AS d_finite, isfinite(ts) AS ts_finite, isfinite(tz) AS tz_finite FROM inf_event ORDER BY id;

SELECT CAST('infinity' AS date) + 1 AS a, CAST('-infinity' AS timestamp) + CAST('1 month' AS interval) AS b, CAST('infinity' AS timestamp) - CAST('1 day' AS interval) AS c;

SELECT CAST('-infinity' AS timestamptz) < CAST('2026-10-27 12:00:00+00' AS timestamptz) AS before, CAST('infinity' AS timestamptz) > CAST('2026-10-27 12:00:00+00' AS timestamptz) AS after;

SELECT CAST('epoch' AS date) AS e, CAST('epoch' AS timestamp) AS t;
