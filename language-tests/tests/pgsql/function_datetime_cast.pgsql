/**
[env]
namespace = true
database = true

[test]
reason = "A text cast to a timestamp or a date is read as the servers write it, with a space or a T before the time, with or without a zone, and a date has no time of day"

[[test.results]]
value = "[{ a: ts'2024-03-15T10:20:30', b: ts'2024-03-15T10:20:30', c: ts'2024-03-15T10:20:00', d: ts'2024-03-15T10:20:30.500' }]"

[[test.results]]
value = "[{ a: pd'2024-03-15', b: pd'2024-03-15', c: d'2024-03-15T08:20:30Z', d: d'2024-03-15T15:50:30Z', e: NULL }]"

[[test.results]]
value = "[{ a: ts'2024-03-15T10:20:30', b: pd'2024-03-15' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: ts'2024-03-15T10:20:30', id: 1 }]"

[[test.results]]
value = "[{ same: true }]"

[[test.results]]
error = "Could not cast into `timestamp` using input `'garbage'`"

*/

SELECT CAST('2024-03-15 10:20:30' AS timestamp) AS a, CAST('2024-03-15T10:20:30' AS timestamp) AS b, CAST('2024-03-15 10:20' AS timestamp) AS c, CAST('2024-03-15 10:20:30.5' AS timestamp) AS d;

SELECT CAST('2024-03-15' AS date) AS a, CAST(CAST('2024-03-15 10:20:30' AS timestamp) AS date) AS b, CAST('2024-03-15 10:20:30+02' AS timestamptz) AS c, CAST('2024-03-15 10:20:30-05:30' AS timestamptz) AS d, CAST(NULL AS timestamp) AS e;

SELECT CAST(s AS timestamp) AS a, CAST(s AS date) AS b FROM (SELECT '2024-03-15 10:20:30' AS s) AS q;

CREATE TABLE cast_ts (id int PRIMARY KEY, at timestamp);

INSERT INTO cast_ts (id, at) VALUES (1, CAST('2024-03-15 10:20:30' AS timestamp));

SELECT at = CAST('2024-03-15T10:20:30Z' AS timestamp) AS same FROM cast_ts;

SELECT CAST('garbage' AS timestamp);
