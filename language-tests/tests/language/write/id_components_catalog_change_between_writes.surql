/**
[env]
namespace = true
database = true

[test]
reason = """
	A write reads the definitions of its table as the catalog holds them when it
	runs. Changing the `id` field between two writes, with ALTER FIELD or with
	REMOVE FIELD, changes what the next write takes for a component: the names
	the old type gave are fields like any other, and the names the new type gives
	are derived from the key and stored in no record. That holds across
	statements and inside one BEGIN ... COMMIT, whose later writes are planned
	before the definition ahead of them has run.
"""

# Across statements: define, write, alter, write.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[reading:['London', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[reading:['Paris', d'2024-01-01T00:00:00Z']]"

# The old names are no longer components, so nothing supplies `place`.
[[test.results]]
error = "Cannot create a record in `reading` without the record id component `place`: set `place`, give the record id, or define a DEFAULT on the `id` field"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00Z', id: reading:['London', d'2024-01-01T00:00:00Z'], place: 'London', temp: 1 }, { at: d'2024-01-01T00:00:00Z', id: reading:['Paris', d'2024-01-01T00:00:00Z'], place: 'Paris', temp: 2 }]"

# Removing the `id` field frees every name, and the key is no longer built from them.
[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'Zed', id: reading:1, temp: 4 }]"

[[test.results]]
value = "[{ id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 1 }, { id: reading:['Paris', d'2024-01-01T00:00:00Z'], temp: 2 }, { city: 'Zed', id: reading:1, temp: 4 }]"

# Inside one transaction. The last write names the old components as fields
# of a record whose key the new type names, so they are stored as fields.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[trip:['London', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[trip:['Paris', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "[trip:['Oslo', d'2024-01-02T00:00:00Z']]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00Z', id: trip:['London', d'2024-01-01T00:00:00Z'], place: 'London', temp: 1 }, { at: d'2024-01-01T00:00:00Z', id: trip:['Paris', d'2024-01-01T00:00:00Z'], place: 'Paris', temp: 2 }, { at: d'2024-01-02T00:00:00Z', city: 'Oslo', id: trip:['Oslo', d'2024-01-02T00:00:00Z'], place: 'Oslo', temp: 3, time: d'2024-01-02T00:00:00Z' }]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1 RETURN VALUE id;

ALTER FIELD id ON reading TYPE [place: string, at: datetime];
CREATE reading SET place = 'Paris', at = d'2024-01-01T00:00:00Z', temp = 2 RETURN VALUE id;
CREATE reading SET city = 'Rome', time = d'2024-01-02T00:00:00Z', temp = 3;
SELECT * FROM reading ORDER BY temp;

REMOVE FIELD id ON reading;
CREATE reading:1 SET city = 'Zed', temp = 4;
SELECT * FROM reading ORDER BY temp;

DEFINE TABLE trip SCHEMALESS;
BEGIN;
DEFINE FIELD id ON trip TYPE [city: string, time: datetime];
CREATE trip SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1 RETURN VALUE id;
ALTER FIELD id ON trip TYPE [place: string, at: datetime];
CREATE trip SET place = 'Paris', at = d'2024-01-01T00:00:00Z', temp = 2 RETURN VALUE id;
CREATE trip:['Oslo', d'2024-01-02T00:00:00Z'] SET city = 'Oslo', time = d'2024-01-02T00:00:00Z', temp = 3 RETURN VALUE id;
COMMIT;
SELECT * FROM trip ORDER BY temp;
