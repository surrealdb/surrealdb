/**
[env]
timeout = 5000

[test]
reason = "A view over math::sum, math::min and math::max skips a record that lacks the field, as the ad hoc GROUP BY does, instead of making the base table reject it; adding, giving, changing and removing the field keeps the view equal to the ad hoc query"
issue = 7447

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 'a', id: person:n0, v: 10 }]"

[[test.results]]
value = "[{ g: 'a', id: person:n1 }]"

[[test.results]]
value = "[{ g: 'a', id: person:n2, v: NULL }]"

[[test.results]]
value = "[{ g: 'b', id: person:n3 }]"

[[test.results]]
value = "[person:n0, person:n1, person:n2, person:n3]"

[[test.results]]
value = "[{ g: 'a', hi: 10, lo: 10, total: 10 }, { g: 'b', hi: -Infinity, lo: Infinity, total: 0 }]"

[[test.results]]
value = "[{ g: 'a', hi: 10, lo: 10, total: 10 }, { g: 'b', hi: -Infinity, lo: Infinity, total: 0 }]"

[[test.results]]
value = "[{ g: 'a', id: person:n1, v: 5 }]"

[[test.results]]
value = "[{ g: 'a', hi: 10, lo: 5, total: 15 }, { g: 'b', hi: -Infinity, lo: Infinity, total: 0 }]"

[[test.results]]
value = "[{ g: 'a', id: person:n0, v: 1 }]"

[[test.results]]
value = "[{ g: 'a', hi: 5, lo: 1, total: 6 }]"

[[test.results]]
value = "[{ g: 'a', id: person:n1 }]"

[[test.results]]
value = "[{ g: 'a', hi: 1, lo: 1, total: 1 }]"

[[test.results]]
value = "[{ g: 'a', id: person:n0, v: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ g: 'a', hi: 3, lo: 3, total: 3 }, { g: 'b', hi: -Infinity, lo: Infinity, total: 0 }]"

[[test.results]]
value = "[{ g: 'a', hi: 3, lo: 3, total: 3 }, { g: 'b', hi: -Infinity, lo: Infinity, total: 0 }]"

[[test.results]]
error = "Incorrect arguments for function math::sum(). Argument 1 was the wrong type. Expected `number` but found `'text'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00Z', g: 'a', id: event:1 }]"

[[test.results]]
value = "[{ g: 'a', id: event:2 }]"

[[test.results]]
value = "[{ at: d'2025-01-01T00:00:00Z', g: 'a', id: event:3 }]"

[[test.results]]
value = "[{ first: d'2024-01-01T00:00:00Z', g: 'a', last: d'2025-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ first: d'2024-01-01T00:00:00Z', g: 'a', last: d'2025-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ at: d'2026-01-01T00:00:00Z', g: 'a', id: event:2 }]"

[[test.results]]
value = "[{ first: d'2024-01-01T00:00:00Z', g: 'a', last: d'2026-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ g: 'a', id: event:2 }]"

[[test.results]]
value = "[{ g: 'a', id: event:3 }]"

[[test.results]]
value = "[{ first: d'2024-01-01T00:00:00Z', g: 'a', last: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ first: NONE, g: 'a', last: NONE }]"

[[test.results]]
value = "[{ first: NONE, g: 'a', last: NONE }]"

*/

DEFINE TABLE person SCHEMALESS;
DEFINE TABLE stats AS SELECT g, math::sum(v) AS total, math::min(v) AS lo, math::max(v) AS hi FROM person GROUP BY g;
CREATE person:n0 SET g = 'a', v = 10;
CREATE person:n1 SET g = 'a';
CREATE person:n2 SET g = 'a', v = NULL;
CREATE person:n3 SET g = 'b';
SELECT VALUE id FROM person ORDER BY id;
SELECT g, total, lo, hi FROM stats ORDER BY g;
SELECT g, math::sum(v) AS total, math::min(v) AS lo, math::max(v) AS hi FROM person GROUP BY g ORDER BY g;
UPDATE person:n1 SET v = 5;
SELECT g, total, lo, hi FROM stats ORDER BY g;
UPDATE person:n0 SET v = 1;
SELECT g, total, lo, hi FROM stats WHERE g = 'a';
UPDATE person:n1 UNSET v;
SELECT g, total, lo, hi FROM stats WHERE g = 'a';
UPDATE person:n0 SET v = 3;
DELETE person:n2;
DELETE person:n1;
SELECT g, total, lo, hi FROM stats ORDER BY g;
SELECT g, math::sum(v) AS total, math::min(v) AS lo, math::max(v) AS hi FROM person GROUP BY g ORDER BY g;
-- A value of the wrong kind is still refused.
CREATE person:n4 SET g = 'a', v = 'text';
-- The same holds for time::max and time::min.
DEFINE TABLE event SCHEMALESS;
DEFINE TABLE span AS SELECT g, time::min(at) AS first, time::max(at) AS last FROM event GROUP BY g;
CREATE event:1 SET g = 'a', at = d'2024-01-01T00:00:00Z';
CREATE event:2 SET g = 'a';
CREATE event:3 SET g = 'a', at = d'2025-01-01T00:00:00Z';
SELECT g, first, last FROM span;
SELECT g, time::min(at) AS first, time::max(at) AS last FROM event GROUP BY g;
UPDATE event:2 SET at = d'2026-01-01T00:00:00Z';
SELECT g, first, last FROM span;
UPDATE event:2 UNSET at;
UPDATE event:3 UNSET at;
SELECT g, first, last FROM span;
DELETE event:2;
DELETE event:1;
SELECT g, first, last FROM span;
SELECT g, time::min(at) AS first, time::max(at) AS last FROM event GROUP BY g;
