/**
[env]
namespace = true
database = true

[test]
reason = """
	On a table whose `id` names its components, a RETURN clause shows the
	components wherever the record it reads exists: the later image of a create
	or an update, the earlier image of an update or a delete, and a DIFF that
	adds, changes or removes the record. No observer is defined on the table, so
	the images come from the statement's own derivation. A create has no earlier
	image and a delete no later one, and the missing image is left as it is, so a
	component asked for by name is NONE there.
"""

# The setup.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# Creating shapes.
[[test.results]]
value = "[{ city: 'London' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-03T00:00:00Z'], temp: 3, time: d'2024-01-03T00:00:00Z' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { city: 'Paris', id: reading:['Paris', d'2024-01-01T00:00:00Z'], temp: 4, time: d'2024-01-01T00:00:00Z' } }]]"

[[test.results]]
value = "[{ city: 'Paris', temp: 5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ city: 'Rome' }]"

[[test.results]]
value = "[{ city: 'Rome', id: reading:['Rome', d'2024-01-02T00:00:00Z'], temp: 8, time: d'2024-01-02T00:00:00Z' }]"

# Updating shapes: both images exist, and only the stored field is in the diff.
[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 1, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 11, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/temp', value: 12 }]]"

[[test.results]]
value = "[{ city: 'London' }]"

# Deleting shapes: no later image, so a component asked for is NONE.
[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ city: 'Paris', id: reading:['Paris', d'2024-01-02T00:00:00Z'], temp: 5, time: d'2024-01-02T00:00:00Z' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: NONE }]]"

[[test.results]]
value = "[{ city: NONE }]"

# What the statements left.
[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-02T00:00:00Z'], temp: 2, time: d'2024-01-02T00:00:00Z' }, { city: 'Rome', id: reading:['Rome', d'2024-01-01T00:00:00Z'], temp: 7, time: d'2024-01-01T00:00:00Z' }, { city: 'Rome', id: reading:['Rome', d'2024-01-02T00:00:00Z'], temp: 8, time: d'2024-01-02T00:00:00Z' }, { city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 13, time: d'2024-01-01T00:00:00Z' }]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

-- Creating shapes: no earlier image.
CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1 RETURN city;
CREATE reading SET city = 'London', time = d'2024-01-02T00:00:00Z', temp = 2 RETURN BEFORE;
CREATE reading SET city = 'London', time = d'2024-01-03T00:00:00Z', temp = 3 RETURN AFTER;
CREATE reading SET city = 'Paris', time = d'2024-01-01T00:00:00Z', temp = 4 RETURN DIFF;
CREATE reading SET city = 'Paris', time = d'2024-01-02T00:00:00Z', temp = 5 RETURN city, temp;
CREATE reading SET city = 'Paris', time = d'2024-01-03T00:00:00Z', temp = 6 RETURN NONE;
INSERT INTO reading { city: 'Rome', time: d'2024-01-01T00:00:00Z', temp: 7 } RETURN city;
UPSERT reading SET city = 'Rome', time = d'2024-01-02T00:00:00Z', temp = 8 RETURN AFTER;

-- Updating shapes: both images.
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 10 RETURN BEFORE;
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 11 RETURN AFTER;
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 12 RETURN DIFF;
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 13 RETURN city;

-- Deleting shapes: no later image.
DELETE reading:['London', d'2024-01-03T00:00:00Z'] RETURN AFTER;
DELETE reading:['Paris', d'2024-01-02T00:00:00Z'] RETURN BEFORE;
DELETE reading:['Paris', d'2024-01-01T00:00:00Z'] RETURN DIFF;
DELETE reading:['Paris', d'2024-01-03T00:00:00Z'] RETURN city;

SELECT * FROM reading ORDER BY temp;
