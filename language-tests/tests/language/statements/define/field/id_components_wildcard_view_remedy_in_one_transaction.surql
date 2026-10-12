/**
[env]
namespace = true
database = true

[test]
reason = """
	The remedy for a view that selects every field: remove it, change the record
	id and define the view again, in one transaction. The view is filled again
	from the source as it stands, so every row it holds carries the names the
	record id gives, and a filter on a component reaches every row. The same
	holds when the names are removed: no row keeps them.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'London', id: v:['London', d'2024-01-01T00:00:00Z'], temp: 10, time: d'2024-01-01T00:00:00Z' }, { city: 'Paris', id: v:['Paris', d'2024-01-01T00:00:00Z'], temp: 20, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ city: 'Paris', id: v:['Paris', d'2024-01-01T00:00:00Z'], temp: 20, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: v:['London', d'2024-01-01T00:00:00Z'], temp: 10 }, { id: v:['Paris', d'2024-01-01T00:00:00Z'], temp: 20 }]"
*/

DEFINE TABLE reading SCHEMALESS;
INSERT INTO reading [
	{ id: ['London', d'2024-01-01T00:00:00Z'], temp: 10 },
	{ id: ['Paris', d'2024-01-01T00:00:00Z'], temp: 20 }
] RETURN NONE;
DEFINE TABLE v AS SELECT * FROM reading;

BEGIN;
REMOVE TABLE v;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE TABLE v AS SELECT * FROM reading;
COMMIT;
SELECT * FROM v ORDER BY temp;
SELECT * FROM v WHERE city = 'Paris';

BEGIN;
REMOVE TABLE v;
REMOVE FIELD id ON reading;
DEFINE TABLE v AS SELECT * FROM reading;
COMMIT;
SELECT * FROM v ORDER BY temp;
