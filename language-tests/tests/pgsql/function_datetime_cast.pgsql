/**
[env]
namespace = true
database = true

[test]
reason = "A text cast to a timestamp or a date is read as the servers write it, with a space or a T before the time, with or without a zone, and a date has no time of day"

[[test.results]]
value = "[{ a: d'2024-03-15T10:20:30Z', b: d'2024-03-15T10:20:30Z', c: d'2024-03-15T10:20:00Z', d: d'2024-03-15T10:20:30.500Z' }]"

[[test.results]]
value = "[{ a: d'2024-03-15T00:00:00Z', b: d'2024-03-15T00:00:00Z', c: d'2024-03-15T08:20:30Z', d: d'2024-03-15T15:50:30Z', e: NULL }]"

[[test.results]]
value = "[{ a: d'2024-03-15T10:20:30Z', b: d'2024-03-15T00:00:00Z' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: d'2024-03-15T10:20:30Z', id: 1 }]"

[[test.results]]
value = "[{ same: true }]"

[[test.results]]
error = "Could not cast into `datetime` using input `'garbage'`"

*/

SELECT CAST('2024-03-15 10:20:30' AS timestamp) AS a, CAST('2024-03-15T10:20:30' AS timestamp) AS b, CAST('2024-03-15 10:20' AS timestamp) AS c, CAST('2024-03-15 10:20:30.5' AS timestamp) AS d;

SELECT CAST('2024-03-15' AS date) AS a, CAST(CAST('2024-03-15 10:20:30' AS timestamp) AS date) AS b, CAST('2024-03-15 10:20:30+02' AS timestamptz) AS c, CAST('2024-03-15 10:20:30-05:30' AS timestamptz) AS d, CAST(NULL AS timestamp) AS e;

SELECT CAST(s AS timestamp) AS a, CAST(s AS date) AS b FROM (SELECT '2024-03-15 10:20:30' AS s) AS q;

CREATE TABLE cast_ts (id int PRIMARY KEY, at timestamp);

INSERT INTO cast_ts (id, at) VALUES (1, CAST('2024-03-15 10:20:30' AS timestamp));

SELECT at = CAST('2024-03-15T10:20:30Z' AS timestamp) AS same FROM cast_ts;

SELECT CAST('garbage' AS timestamp);
