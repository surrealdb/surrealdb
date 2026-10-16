/**
[env]
namespace = true
database = true

[test]
reason = """
	A record id that names its components derives each one as a layer column
	reading one element of the id, labelled `@key` so that it is told from a
	computed field that reads the id. A conjunct over a component is judged
	over the id beneath the layer, so a statement that reads the component only
	there has no layer left, and one that reads it elsewhere keeps the column.
	The rows are the same whichever way the plan goes.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

# A conjunct is the only read of `city`: no layer is left.
[[test.results]]
match = '''!string::contains($result, "ProjectLayer") AND !string::contains($result, "@key") AND string::contains($result, "city = 'London'")'''
error = false

[[test.results]]
value = "[{ temp: 4 }, { temp: 5 }]"

# The same for a `VALUE` projection.
[[test.results]]
match = '''!string::contains($result, "ProjectLayer") AND !string::contains($result, "@key") AND string::contains($result, "city = 'London'")'''
error = false

[[test.results]]
value = "[4, 5]"

# `*` reads the whole row: both components are derived, and labelled.
[[test.results]]
match = '''string::contains($result, "city = id[0] : string @key") AND string::contains($result, "time = id[1] : datetime @key") AND string::contains($result, "city = 'London'")'''
error = false

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], label: 'London-4', temp: 4, time: d'2024-01-01T00:00:00Z' }, { city: 'London', id: reading:['London', d'2024-01-02T00:00:00Z'], label: 'London-5', temp: 5, time: d'2024-01-02T00:00:00Z' }]"

# Two components in one statement.
[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-02T00:00:00Z'], label: 'London-5', temp: 5, time: d'2024-01-02T00:00:00Z' }]"

# A component the ordering reads keeps its column; the one nothing reads is not derived.
[[test.results]]
match = '''string::contains($result, "time = id[1] : datetime @key") AND !string::contains($result, "city = ")'''
error = false

[[test.results]]
value = "[{ temp: 5 }, { temp: 7 }, { temp: 4 }, { temp: 6 }]"

# A grouping reads its key as a column.
[[test.results]]
match = '''string::contains($result, "city = id[0] : string @key") AND !string::contains($result, "time = ")'''
error = false

[[test.results]]
value = "[{ city: 'London', n: 2 }, { city: 'Paris', n: 2 }]"

# A disjunction over a component and a computed field is one conjunct and stays above the layers.
[[test.results]]
match = '''string::contains($result, "city = id[0] : string @key") AND string::contains($result, "label = ") AND string::contains($result, "city = 'London' OR label = 'Paris-6'")'''
error = false

[[test.results]]
value = "[{ temp: 4 }, { temp: 5 }, { temp: 6 }]"

# A read of the row as a whole stays above the layers too.
[[test.results]]
match = '''string::contains($result, "city = id[0] : string @key") AND string::contains($result, "$this.city = 'Paris'")'''
error = false

[[test.results]]
value = "[{ temp: 6 }, { temp: 7 }]"

# A computed field that reads a component keeps the column it reads.
[[test.results]]
match = '''string::contains($result, "city = id[0] : string @key") AND string::contains($result, "label = ") AND string::contains($result, "city = 'London'")'''
error = false

[[test.results]]
value = "['London-4', 'London-5']"
*/

DEFINE TABLE reading SCHEMAFULL;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD temp ON reading TYPE int;
DEFINE FIELD label ON reading TYPE string COMPUTED string::concat(city, '-', temp);

CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 4 RETURN NONE;
CREATE reading:['London', d'2024-01-02T00:00:00Z'] SET temp = 5 RETURN NONE;
CREATE reading:['Paris', d'2024-01-01T00:00:00Z'] SET temp = 6 RETURN NONE;
CREATE reading:['Paris', d'2024-01-02T00:00:00Z'] SET temp = 7 RETURN NONE;

EXPLAIN SELECT temp FROM reading WHERE city = 'London';
SELECT temp FROM reading WHERE city = 'London' ORDER BY temp;

EXPLAIN SELECT VALUE temp FROM reading WHERE city = 'London';
SELECT VALUE temp FROM reading WHERE city = 'London' ORDER BY temp;

EXPLAIN SELECT * FROM reading WHERE city = 'London';
SELECT * FROM reading WHERE city = 'London' ORDER BY time;
SELECT * FROM reading WHERE city = 'London' AND time > d'2024-01-01T00:00:00Z';

EXPLAIN SELECT temp FROM reading ORDER BY time;
SELECT temp FROM reading ORDER BY time DESC, temp;

EXPLAIN SELECT city, count() AS n FROM reading GROUP BY city;
SELECT city, count() AS n FROM reading GROUP BY city ORDER BY city;

EXPLAIN SELECT temp FROM reading WHERE city = 'London' OR label = 'Paris-6';
SELECT temp FROM reading WHERE city = 'London' OR label = 'Paris-6' ORDER BY temp;

EXPLAIN SELECT temp FROM reading WHERE $this.city = 'Paris';
SELECT temp FROM reading WHERE $this.city = 'Paris' ORDER BY temp;

EXPLAIN SELECT VALUE label FROM reading WHERE city = 'London';
SELECT VALUE label FROM reading WHERE city = 'London' ORDER BY label;
