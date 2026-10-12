/**
[env]
namespace = true
database = true

[test]
reason = """
	A view over a table whose `id` names its components reads them as it reads
	any field of the source, while it is defined and while it is maintained. The
	grouped view's rows are keyed by the component it groups by, its extremes
	are recomputed from the group's remaining records when a write lowers one,
	and a group whose last record goes is removed. The view that copies the
	records is filtered by a component, so the rows it holds follow the records
	the condition admits. No other observer is defined on the source: nothing but
	the view asks for the components of the images a write hands it.
"""

# The setup: two records, then the views, which are filled from them.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'London', id: by_city:['London'], n: 1, top: 10 }, { city: 'Paris', id: by_city:['Paris'], n: 1, top: 20 }]"

[[test.results]]
value = "[{ city: 'London', id: london:['London', d'2024-01-01T00:00:00Z'], temp: 10, time: d'2024-01-01T00:00:00Z' }]"

# A create joins a group and is admitted by the condition.
[[test.results]]
value = "[]"

[[test.results]]
value = "[{ city: 'London', id: by_city:['London'], n: 2, top: 30 }, { city: 'Paris', id: by_city:['Paris'], n: 1, top: 20 }]"

[[test.results]]
value = "[{ city: 'London', id: london:['London', d'2024-01-01T00:00:00Z'], temp: 10, time: d'2024-01-01T00:00:00Z' }, { city: 'London', id: london:['London', d'2024-01-02T00:00:00Z'], temp: 30, time: d'2024-01-02T00:00:00Z' }]"

# An update that lowers the group's extreme recomputes it from the group.
[[test.results]]
value = "[]"

[[test.results]]
value = "[{ city: 'London', id: by_city:['London'], n: 2, top: 10 }, { city: 'Paris', id: by_city:['Paris'], n: 1, top: 20 }]"

[[test.results]]
value = "[{ city: 'London', id: london:['London', d'2024-01-02T00:00:00Z'], temp: 5, time: d'2024-01-02T00:00:00Z' }, { city: 'London', id: london:['London', d'2024-01-01T00:00:00Z'], temp: 10, time: d'2024-01-01T00:00:00Z' }]"

# A delete leaves its group, which is gone with its last record.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ city: 'London', id: by_city:['London'], n: 1, top: 5 }]"

[[test.results]]
value = "[{ city: 'London', id: london:['London', d'2024-01-02T00:00:00Z'], temp: 5, time: d'2024-01-02T00:00:00Z' }]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 10 RETURN NONE;
CREATE reading SET city = 'Paris', time = d'2024-01-01T00:00:00Z', temp = 20 RETURN NONE;

DEFINE TABLE by_city AS SELECT city, count() AS n, math::max(temp) AS top FROM reading GROUP BY city;
DEFINE TABLE london AS SELECT city, time, temp FROM reading WHERE city = 'London';

SELECT * FROM by_city ORDER BY city;
SELECT * FROM london ORDER BY temp;

-- A create.
CREATE reading SET city = 'London', time = d'2024-01-02T00:00:00Z', temp = 30 RETURN NONE;
SELECT * FROM by_city ORDER BY city;
SELECT * FROM london ORDER BY temp;

-- An update that takes the extreme of its group down.
UPDATE reading:['London', d'2024-01-02T00:00:00Z'] SET temp = 5 RETURN NONE;
SELECT * FROM by_city ORDER BY city;
SELECT * FROM london ORDER BY temp;

-- Deletes: the last record of a group, and a record that held an extreme.
DELETE reading:['Paris', d'2024-01-01T00:00:00Z'] RETURN NONE;
DELETE reading:['London', d'2024-01-01T00:00:00Z'] RETURN NONE;
SELECT * FROM by_city ORDER BY city;
SELECT * FROM london ORDER BY temp;
