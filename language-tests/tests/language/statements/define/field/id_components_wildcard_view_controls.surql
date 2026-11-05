/**
[env]
namespace = true
database = true

[test]
reason = """
	Only a view that selects every field holds the components by the names its
	rows were written under. An aggregate view and a view that selects named
	fields other than the components read no component, so their rows do not
	block naming, renaming or removing the components. A view that selects
	every field and a component by name is reported for the component it
	reads, before the fields it selects as a whole.
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
value = "[{ id: agg:[10], n: 1, temp: 10 }]"

[[test.results]]
value = "[{ id: some:['London', d'2024-01-01T00:00:00Z'], temp: 10 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `reading`: the view `v` reads its component `city`"
*/

DEFINE TABLE reading SCHEMALESS;
INSERT INTO reading { id: ['London', d'2024-01-01T00:00:00Z'], temp: 10 } RETURN NONE;
DEFINE TABLE agg AS SELECT temp, count() AS n FROM reading GROUP BY temp;
DEFINE TABLE some AS SELECT temp FROM reading;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
ALTER FIELD id ON reading TYPE [place: string, time: datetime];
REMOVE FIELD id ON reading;
SELECT * FROM agg;
SELECT * FROM some;

-- Every field and a component by name: the name is reported.
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE TABLE v AS SELECT *, city AS c FROM reading WHERE temp > 5;
REMOVE FIELD id ON reading;
