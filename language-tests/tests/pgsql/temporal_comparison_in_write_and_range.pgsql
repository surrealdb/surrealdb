/**
[env]
namespace = true
database = true

[test]
reason = "A text literal beside a timestamptz column is read as a timestamptz in a DELETE or UPDATE predicate and in BETWEEN and IN, as it is in a comparison in a SELECT"

[[test.results]]
value = "NONE"

[[test.results]]
match = "array::len($result) == 4"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: 3 }]"

[[test.results]]
value = "[{ id: 2 }]"

[[test.results]]
value = "[{ id: 4 }]"

[[test.results]]
value = "[{ id: 2 }, { id: 3 }, { id: 4 }]"
*/

CREATE TABLE events (id INT PRIMARY KEY, at TIMESTAMPTZ);

INSERT INTO events (id, at) VALUES (1, CAST('2024-01-01T00:00:00Z' AS timestamptz)), (2, CAST('2024-06-01T00:00:00Z' AS timestamptz)), (3, CAST('2024-09-01T00:00:00Z' AS timestamptz)), (4, CAST('2024-12-01T00:00:00Z' AS timestamptz));

DELETE FROM events WHERE at < '2024-03-01' RETURNING id;

DELETE FROM events WHERE at > now() + '30 days'::interval RETURNING id;

UPDATE events SET at = CAST('2025-01-01T00:00:00Z' AS timestamptz) WHERE at > '2024-08-01' AND at < '2024-10-01' RETURNING id;

SELECT id FROM events WHERE at BETWEEN '2024-05-01' AND '2024-07-01' ORDER BY id;

SELECT id FROM events WHERE at IN ('2024-12-01 00:00:00+00') ORDER BY id;

SELECT id FROM events ORDER BY id;
