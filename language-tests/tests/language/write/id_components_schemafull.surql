/**
[env]
namespace = true
database = true

[test]
reason = """
	On a SCHEMAFULL table the fields that name the components of the record id
	are defined by the id and are never stored, so a write may name them without
	declaring them and without tripping the check for undefined fields, and a
	field that is not defined is refused as before. The components are judged
	against the key before the table's own fields are processed: a record that
	contradicts its key is refused with that, even when another field of the
	same write would also be refused.
"""

# The setup.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# The components are written without being declared.
[[test.results]]
value = "[reading:['London', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "[reading:['London', d'2024-01-02T00:00:00Z']]"

# A field the table does not define is still refused.
[[test.results]]
error = "Found field 'extra', but no such field exists for table 'reading'"

# An update restating a component, and one contradicting it.
[[test.results]]
value = "[6]"

[[test.results]]
error = "Found 'Paris' for the `city` field, but it is part of the record id of `reading` and does not match it"

# The key is judged before the other fields: the datetime is what is refused.
[[test.results]]
error = "Found d'2025-01-01T00:00:00Z' for the `time` field, but it is part of the record id of `reading` and does not match it"

# A row read with `*` is written back whole.
[[test.results]]
value = "[6]"

# The components are read as fields, and were stored in no record.
[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-02T00:00:00Z'], temp: 5, time: d'2024-01-02T00:00:00Z' }, { city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 6, time: d'2024-01-01T00:00:00Z' }]"
*/

DEFINE TABLE reading SCHEMAFULL;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD temp ON reading TYPE int;

CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 4 RETURN VALUE id;
CREATE reading:['London', d'2024-01-02T00:00:00Z'] CONTENT { city: 'London', time: d'2024-01-02T00:00:00Z', temp: 5 } RETURN VALUE id;
CREATE reading SET city = 'Paris', time = d'2024-01-01T00:00:00Z', temp = 6, extra = 1;

UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET city = 'London', temp = 6 RETURN VALUE temp;
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET city = 'Paris';
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET time = d'2025-01-01T00:00:00Z', temp = 'x';
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] CONTENT (SELECT * FROM ONLY reading:['London', d'2024-01-01T00:00:00Z']) RETURN VALUE temp;

SELECT * FROM reading ORDER BY temp;
