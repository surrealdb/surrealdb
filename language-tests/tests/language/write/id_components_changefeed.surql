/**
[env]
namespace = true
database = true

[test]
reason = """
	A change feed records the image a write leaves, and the image of a table
	whose `id` names its components holds them as it holds a COMPUTED field's
	value: derived from the id, though stored in no record. The earlier image
	is derived as the later one is, so the diff an `INCLUDE ORIGINAL` feed keeps
	for an update names only what the write changed, and the original a delete
	keeps holds the components. The table has no event, view or subscription:
	the feed alone asks for the components.
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

# A feed that keeps the later image.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
match = '''
array::any($result, |$entry| array::any($entry.changes, |$c| $c.update == { city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 1, time: d'2024-01-01T00:00:00Z' }))
&& array::any($result, |$entry| array::any($entry.changes, |$c| $c.update == { city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 2, time: d'2024-01-01T00:00:00Z' }))
&& array::any($result, |$entry| array::any($entry.changes, |$c| $c.delete == { id: reading:['London', d'2024-01-01T00:00:00Z'] }))
'''
error = false

# A feed that keeps the earlier image as well.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
match = '''
array::any($result, |$entry| array::any($entry.changes, |$c| $c.update == { city: 'London', id: kept:['London', d'2024-01-01T00:00:00Z'], temp: 1, time: d'2024-01-01T00:00:00Z' }))
&& array::any($result, |$entry| array::any($entry.changes, |$c| ($c.current == { city: 'London', id: kept:['London', d'2024-01-01T00:00:00Z'], temp: 2, time: d'2024-01-01T00:00:00Z' } && $c.update == [{ op: 'replace', path: '/temp', value: 1 }])))
&& array::any($result, |$entry| array::any($entry.changes, |$c| $c.delete == { id: kept:['London', d'2024-01-01T00:00:00Z'], original: { city: 'London', id: kept:['London', d'2024-01-01T00:00:00Z'], temp: 2, time: d'2024-01-01T00:00:00Z' } }))
'''
error = false
*/

DEFINE TABLE reading SCHEMALESS CHANGEFEED 1h;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE TABLE kept SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL;
DEFINE FIELD id ON kept TYPE [city: string, time: datetime];

CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1 RETURN NONE;
UPDATE reading SET temp = 2 RETURN NONE;
DELETE reading RETURN NONE;
SHOW CHANGES FOR TABLE reading SINCE 1;

CREATE kept SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1 RETURN NONE;
UPDATE kept SET temp = 2 RETURN NONE;
DELETE kept RETURN NONE;
SHOW CHANGES FOR TABLE kept SINCE 1;
