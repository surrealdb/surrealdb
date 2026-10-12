/**
[env]
namespace = true
database = true

[test]
reason = """
	An UPSERT on a table whose `id` names its components is an upsert by key:
	the key of the record it creates is made of the components it writes, so a
	repeat of the statement finds the record it made and updates it, and the
	fields that restate the key pass the update's check. A `WHERE` that matches
	nothing creates from the `SET` alone, so it must supply the components, an
	id or a DEFAULT. A component that is missing is refused as a create refuses
	it, and a field that contradicts the key is refused on either leg.
"""

# The setup.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# By key fields: the first creates, the repeat updates the same record.
[[test.results]]
value = "[1]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[2]"

# By id: the same, with the key fields restated on the update leg.
[[test.results]]
value = "[3]"

[[test.results]]
value = "[4]"

[[test.results]]
error = "Found 'Rome' for the `city` field, but it is part of the record id of `reading` and does not match it"

# A component nothing supplies.
[[test.results]]
error = "Cannot create a record in `reading` without the record id component `time`: set `time`, give the record id, or define a DEFAULT on the `id` field"

# A WHERE that matches nothing creates from the SET.
[[test.results]]
value = "[reading:['Rome', d'2024-03-01T00:00:00Z']]"

[[test.results]]
value = "[{ count: 3 }]"

# A WHERE that matches updates the record, whose key the SET restates.
[[test.results]]
value = "[7]"

[[test.results]]
value = "[7]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

UPSERT reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1 RETURN VALUE temp;
UPSERT reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 2 RETURN VALUE temp;
SELECT VALUE temp FROM reading;

UPSERT reading:['Paris', d'2024-01-01T00:00:00Z'] SET temp = 3 RETURN VALUE temp;
UPSERT reading:['Paris', d'2024-01-01T00:00:00Z'] SET city = 'Paris', temp = 4 RETURN VALUE temp;
UPSERT reading:['Paris', d'2024-01-01T00:00:00Z'] SET city = 'Rome';

UPSERT reading SET city = 'Paris', temp = 5;

UPSERT reading SET city = 'Rome', time = d'2024-03-01T00:00:00Z', temp = 6 WHERE temp > 100 RETURN VALUE id;
SELECT count() FROM reading GROUP ALL;

UPSERT reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 7 WHERE temp = 2 RETURN VALUE temp;
SELECT VALUE temp FROM reading WHERE city = 'London';
