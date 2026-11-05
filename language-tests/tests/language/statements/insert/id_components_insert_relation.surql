/**
[env]
namespace = true
database = true

[test]
reason = """
	INSERT RELATION into a table whose `id` names its components takes the key
	from the components its values carry, as INSERT does: the value is the
	statement's payload, merged onto the record once the key is settled, so the
	key is read from it. A value that lacks a component is refused naming that
	component, an explicit id is held to the components the value restates, and
	the stored edges hold no copy of a component.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[reading:['London', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "[reading:['London', d'2024-01-02T00:00:00Z'], reading:['Paris', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "[reading:['Rome', d'2024-01-01T00:00:00Z']]"

[[test.results]]
error = "Cannot create a record in `reading` without the record id component `time`: set `time`, give the record id, or define a DEFAULT on the `id` field"

[[test.results]]
error = "Cannot create a record in `reading` without the record id component `city`: set `city`, give the record id, or define a DEFAULT on the `id` field"

[[test.results]]
error = "Found 'Bern' for the `city` field, but it is part of the record id of `reading` and does not match it"

[[test.results]]
value = "[reading:['Oslo', d'2024-01-01T00:00:00Z']]"

[[test.results]]
error = "Couldn't coerce value for the record id component `time` of `reading`: Expected `datetime` but found `'2024-01-01T00:00:00Z'`"

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], in: person:a, out: person:b, temp: 1, time: d'2024-01-01T00:00:00Z' }, { city: 'London', id: reading:['London', d'2024-01-02T00:00:00Z'], in: person:a, out: person:b, temp: 2, time: d'2024-01-02T00:00:00Z' }, { city: 'Oslo', id: reading:['Oslo', d'2024-01-01T00:00:00Z'], in: person:a, out: person:b, temp: 7, time: d'2024-01-01T00:00:00Z' }, { city: 'Paris', id: reading:['Paris', d'2024-01-01T00:00:00Z'], in: person:b, out: person:a, temp: 3, time: d'2024-01-01T00:00:00Z' }, { city: 'Rome', id: reading:['Rome', d'2024-01-01T00:00:00Z'], in: person:a, out: person:b, temp: 4, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[[person:b, person:b, person:b, person:b]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: reading:['London', d'2024-01-01T00:00:00Z'], in: person:a, out: person:b, temp: 1 }, { id: reading:['London', d'2024-01-02T00:00:00Z'], in: person:a, out: person:b, temp: 2 }, { id: reading:['Oslo', d'2024-01-01T00:00:00Z'], in: person:a, out: person:b, temp: 7 }, { id: reading:['Paris', d'2024-01-01T00:00:00Z'], in: person:b, out: person:a, temp: 3 }, { id: reading:['Rome', d'2024-01-01T00:00:00Z'], in: person:a, out: person:b, temp: 4 }]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
CREATE person:a, person:b RETURN NONE;
INSERT RELATION INTO reading { in: person:a, out: person:b, city: 'London', time: d'2024-01-01T00:00:00Z', temp: 1 } RETURN VALUE id;
INSERT RELATION INTO reading [{ in: person:a, out: person:b, city: 'London', time: d'2024-01-02T00:00:00Z', temp: 2 }, { in: person:b, out: person:a, city: 'Paris', time: d'2024-01-01T00:00:00Z', temp: 3 }] RETURN VALUE id;
INSERT RELATION INTO reading (in, out, city, time, temp) VALUES (person:a, person:b, 'Rome', d'2024-01-01T00:00:00Z', 4) RETURN VALUE id;
INSERT RELATION INTO reading { in: person:a, out: person:b, city: 'Oslo', temp: 5 };
INSERT RELATION INTO reading { in: person:a, out: person:b, time: d'2024-01-01T00:00:00Z', temp: 5 };
INSERT RELATION INTO reading { id: ['Oslo', d'2024-01-01T00:00:00Z'], in: person:a, out: person:b, city: 'Bern', temp: 6 };
INSERT RELATION INTO reading { id: ['Oslo', d'2024-01-01T00:00:00Z'], in: person:a, out: person:b, city: 'Oslo', temp: 7 } RETURN VALUE id;
INSERT RELATION INTO reading { in: person:a, out: person:b, city: 'Lima', time: '2024-01-01T00:00:00Z', temp: 8 };
SELECT * FROM reading ORDER BY id;
SELECT VALUE ->reading->person FROM person:a;
DEFINE FIELD OVERWRITE id ON reading TYPE [string, datetime];
SELECT * FROM reading ORDER BY id;
