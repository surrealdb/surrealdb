/**
[env]
namespace = true
database = true

[test]
reason = """
	The fields that name the components of a record id are derived from it and
	cannot be changed by writing them. An update may restate a component, with a
	value that is the record id's own once converted as a key element would be,
	and then nothing of it is stored; any other value is refused with the value
	that was written. So is removing a component, assigning NONE to it, writing
	beneath it or growing it, whichever clause form does so: SET, CONTENT, MERGE,
	REPLACE, PATCH or UNSET. A row read with `*` can be written back as CONTENT.
"""

# The setup.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

# Writing the other fields.
[[test.results]]
value = "[5]"

# A different value is refused, and the key's own value is not shown.
[[test.results]]
error = "Found 'Paris' for the `city` field, but it is part of the record id of `reading` and does not match it"

# The record id's own value is accepted.
[[test.results]]
value = "[6]"

[[test.results]]
value = "[7]"

# A string is not a datetime, so it does not restate one.
[[test.results]]
error = "Found '2024-01-01T00:00:00Z' for the `time` field, but it is part of the record id of `reading` and does not match it"

# A row read with `*` is written back as it is.
[[test.results]]
value = "[7]"

# MERGE.
[[test.results]]
value = "[8]"

[[test.results]]
error = "Found d'2025-01-01T00:00:00Z' for the `time` field, but it is part of the record id of `reading` and does not match it"

# PATCH.
[[test.results]]
value = "[8]"

[[test.results]]
error = "Found 'Paris' for the `city` field, but it is part of the record id of `reading` and does not match it"

# Removing a component.
[[test.results]]
error = "Found NONE for the `city` field, but it is part of the record id of `reading` and does not match it"

[[test.results]]
error = "Found NONE for the `city` field, but it is part of the record id of `reading` and does not match it"

# Growing it.
[[test.results]]
error = "Found 'Londonx' for the `city` field, but it is part of the record id of `reading` and does not match it"

# Writing beneath it.
[[test.results]]
match = "$error = /Found .* for the .city. field, but it is part of the record id of .reading. and does not match it/"
error = true

# REPLACE.
[[test.results]]
error = "Found 'Paris' for the `city` field, but it is part of the record id of `reading` and does not match it"

# A write that mixes both kinds of field.
[[test.results]]
value = "[9]"

# The refused writes changed nothing, and nothing was stored for a component.
[[test.results]]
value = "[9]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 9 }]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 4 RETURN NONE;

UPDATE reading SET temp = 5 RETURN VALUE temp;
UPDATE reading SET city = 'Paris';
UPDATE reading SET city = 'London', temp = 6 RETURN VALUE temp;
UPDATE reading SET time = d'2024-01-01T00:00:00Z', temp = 7 RETURN VALUE temp;
UPDATE reading SET time = '2024-01-01T00:00:00Z';

UPDATE reading CONTENT (SELECT * FROM ONLY reading:['London', d'2024-01-01T00:00:00Z']) RETURN VALUE temp;

UPDATE reading MERGE { city: 'London', temp: 8 } RETURN VALUE temp;
UPDATE reading MERGE { time: d'2025-01-01T00:00:00Z' };

UPDATE reading PATCH [{ op: 'replace', path: '/city', value: 'London' }] RETURN VALUE temp;
UPDATE reading PATCH [{ op: 'replace', path: '/city', value: 'Paris' }];

UPDATE reading UNSET city;
UPDATE reading SET city = NONE;
UPDATE reading SET city += 'x';
UPDATE reading SET city.x = 1;
UPDATE reading REPLACE { city: 'Paris', temp: 1 };

UPDATE reading SET temp = 9, city = 'London' RETURN VALUE temp;

SELECT VALUE temp FROM reading;
DEFINE FIELD OVERWRITE id ON reading TYPE [string, datetime];
SELECT * FROM reading;
