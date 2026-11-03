/**
[env]
namespace = true
database = true
imports = ["pgsql/_temporal_columns.surql"]

[test]
reason = "A datetime column accepts date and timestamp values and keeps reading them as datetimes, a date column accepts a datetime at exactly midnight UTC and refuses any other with the cast to use, and a timestamp column accepts a datetime"

[[test.results]]
value = "[{ at: d'2026-10-27T00:00:00Z', day: pd'2026-10-27', id: 1, wall: ts'2026-10-27T12:30:00' }]"

[[test.results]]
value = "[{ at: d'2026-10-27T12:30:00Z', day: pd'2026-10-28', id: 2, wall: ts'2026-10-27T00:00:00' }]"

[[test.results]]
error = "Couldn't coerce value for field `day` of `temporal_compat:3`: Expected `date (a datetime that is not midnight UTC needs the cast `<date>`)` but found `d'2026-10-27T12:30:00Z'`"

[[test.results]]
value = "[{ at: d'2026-10-27T00:00:00Z', day: pd'2026-10-27', wall: ts'2026-10-27T12:30:00' }, { at: d'2026-10-27T12:30:00Z', day: pd'2026-10-28', wall: ts'2026-10-27T00:00:00' }]"

[[test.results]]
value = "[{ at: 'timestamp with time zone', day: 'date', wall: 'timestamp without time zone' }, { at: 'timestamp with time zone', day: 'date', wall: 'timestamp without time zone' }]"

*/

INSERT INTO temporal_compat (id, at, wall, day) VALUES (1, CAST('2026-10-27' AS date), CAST('2026-10-27 12:30:00+00' AS timestamptz), CAST('2026-10-27 00:00:00+00' AS timestamptz));

INSERT INTO temporal_compat (id, at, wall, day) VALUES (2, CAST('2026-10-27 12:30:00' AS timestamp), CAST('2026-10-27' AS date), CAST('2026-10-28' AS date));

INSERT INTO temporal_compat (id, at, wall, day) VALUES (3, CAST('2026-10-27' AS date), CAST('2026-10-27' AS date), CAST('2026-10-27 12:30:00+00' AS timestamptz));

SELECT at, wall, day FROM temporal_compat ORDER BY id;

SELECT pg_typeof(at) AS at, pg_typeof(wall) AS wall, pg_typeof(day) AS day FROM temporal_compat ORDER BY id;
