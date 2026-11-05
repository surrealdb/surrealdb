/**
[env]
namespace = true
database = true

[test]
reason = """
	MERGE removes a field it gives NONE, as UNSET does, so merging NONE onto a
	component of the record id is refused like UNSET of it and SET of NONE to
	it, with the record left as it was. A merge that restates the component, or
	names only other fields, is accepted. CONTENT and REPLACE give the whole
	body, so a component they leave out, or give NONE, stays derived from the
	key. On a table whose `id` names nothing, MERGE of NONE still removes the
	field.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Found NONE for the `city` field, but it is part of the record id of `reading` and does not match it"

[[test.results]]
error = "Found NONE for the `time` field, but it is part of the record id of `reading` and does not match it"

[[test.results]]
error = "Found NONE for the `city` field, but it is part of the record id of `reading` and does not match it"

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 3, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 4, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 5, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 6, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: plain:1, temp: 1 }]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 1 RETURN NONE;
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] MERGE { city: NONE, temp: 2 };
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] MERGE { time: NONE };
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] UNSET city;
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] MERGE { city: 'London', temp: 3 } RETURN AFTER;
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] MERGE { temp: 4 } RETURN AFTER;
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] CONTENT { city: NONE, temp: 5 } RETURN AFTER;
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] REPLACE { temp: 6 } RETURN AFTER;
DEFINE TABLE plain SCHEMALESS;
CREATE plain:1 SET city = 'London', temp = 1 RETURN NONE;
UPDATE plain:1 MERGE { city: NONE } RETURN AFTER;
