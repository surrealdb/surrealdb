/**
[env]
namespace = true
database = true

[test]
reason = """
	The fields that name the components of a record id are derived from the id
	and stored in no record, so everything that observes a write is handed them
	as it is handed a COMPUTED field's value: the event reads them from `$before`
	and `$after`, and the RETURN of the statement shows them. The table here
	has no other computed field and no other observer, so nothing but the id's
	own components asks for the derivation. A create has no earlier image and a
	delete no later one; the missing one is left as it is.

	The statements that return a record end with the same images the event
	records: a create's DIFF replaces the whole record, an update's names only
	what changed, and a delete's removes it.
"""

# The setup.
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

# A create: the later image shows the components.
[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 1, time: d'2024-01-01T00:00:00Z' }]"

# An update: the earlier image shows them too, and the diff holds only temp.
[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 1, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/temp', value: 3 }]]"

# A delete removes the whole record, and a create's diff adds all of it.
[[test.results]]
value = "[[{ op: 'replace', path: '', value: NONE }]]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { city: 'Paris', id: reading:['Paris', d'2024-01-02T00:00:00Z'], temp: 5, time: d'2024-01-02T00:00:00Z' } }]]"

# What the events recorded, in the order of the event and of the temperature.
[[test.results]]
value = """[
	{ after_city: 'London', after_temp: 1, after_time: d'2024-01-01T00:00:00Z', before_city: NONE, before_time: NONE, ev: 'CREATE' },
	{ after_city: 'Paris', after_temp: 5, after_time: d'2024-01-02T00:00:00Z', before_city: NONE, before_time: NONE, ev: 'CREATE' },
	{ after_city: NONE, after_temp: NONE, after_time: NONE, before_city: 'London', before_time: d'2024-01-01T00:00:00Z', ev: 'DELETE' },
	{ after_city: 'Paris', after_temp: NONE, after_time: d'2024-01-02T00:00:00Z', before_city: NONE, before_time: NONE, ev: 'PARIS' },
	{ after_city: 'London', after_temp: 2, after_time: d'2024-01-01T00:00:00Z', before_city: 'London', before_time: d'2024-01-01T00:00:00Z', ev: 'UPDATE' },
	{ after_city: 'London', after_temp: 3, after_time: d'2024-01-01T00:00:00Z', before_city: 'London', before_time: d'2024-01-01T00:00:00Z', ev: 'UPDATE' }
]"""
*/

DEFINE TABLE elog SCHEMALESS;
DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

-- An event that records both images of every write.
DEFINE EVENT seen ON reading THEN (
	CREATE elog SET ev = $event, before_city = $before.city, after_city = $after.city,
		before_time = $before.time, after_time = $after.time, after_temp = $after.temp
);

-- An event whose condition reads a component of the later image.
DEFINE EVENT paris ON reading WHEN $after.city = 'Paris' THEN (
	CREATE elog SET ev = 'PARIS', after_city = $after.city, after_time = $after.time
);

CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1 RETURN AFTER;
UPDATE reading SET temp = 2 RETURN BEFORE;
UPDATE reading SET temp = 3 RETURN DIFF;
DELETE reading RETURN DIFF;
CREATE reading SET city = 'Paris', time = d'2024-01-02T00:00:00Z', temp = 5 RETURN DIFF;

SELECT ev, before_city, after_city, before_time, after_time, after_temp FROM elog ORDER BY ev, after_temp;
