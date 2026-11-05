/**
[env]
namespace = true
database = true

[test]
reason = """
	A write under OPTION IMPORT, which replays records as they were exported and
	derives nothing around them, settles the components of a record id as any
	write does: the key an explicit id names is converted element by element and
	held to its components, a key that cannot be made is refused naming the
	component it lacks, a field that contradicts the key is refused, and a field
	that restates it is cut, so the record is stored without it. The stored
	bodies are read by retyping the id to one with no names, which derives
	nothing.
"""

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
error = "Found 'Rome' for the `city` field, but it is part of the record id of `reading` and does not match it"

[[test.results]]
match = "$error = /Couldn't coerce value for the record id component .time. of .reading.: /"
error = true

[[test.results]]
error = "The record id of `reading` has the components [city, time], but 1 value was given"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot create a record in `reading` without the record id component `city`: set `city`, give the record id, or define a DEFAULT on the `id` field"

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 4, time: d'2024-01-01T00:00:00Z' }, { city: 'London', id: reading:['London', d'2024-01-02T00:00:00Z'], temp: 5, time: d'2024-01-02T00:00:00Z' }, { city: 'Paris', id: reading:['Paris', d'2024-01-03T00:00:00Z'], temp: 9, time: d'2024-01-03T00:00:00Z' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 4 }, { id: reading:['London', d'2024-01-02T00:00:00Z'], temp: 5 }, { id: reading:['Paris', d'2024-01-03T00:00:00Z'], temp: 9 }]"
*/

OPTION IMPORT;
DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

-- Records as an export writes them, and a copy of the key that a body holds.
CREATE reading:['London', d'2024-01-01T00:00:00Z'] CONTENT { temp: 4 };
CREATE reading:['London', d'2024-01-02T00:00:00Z'] CONTENT { city: 'London', time: d'2024-01-02T00:00:00Z', temp: 5 };

-- What a replay refuses as a write does.
CREATE reading:['Paris', d'2024-01-01T00:00:00Z'] CONTENT { city: 'Rome', temp: 6 };
CREATE reading:['Paris', 5] CONTENT { temp: 7 };
CREATE reading:['Paris'] CONTENT { temp: 8 };

-- A key made of the components, and one that cannot be made.
CREATE reading CONTENT { city: 'Paris', time: d'2024-01-03T00:00:00Z', temp: 9 };
CREATE reading CONTENT { temp: 10 };

SELECT * FROM reading ORDER BY temp;

DEFINE FIELD OVERWRITE id ON reading TYPE [string, datetime];
SELECT * FROM reading ORDER BY temp;
