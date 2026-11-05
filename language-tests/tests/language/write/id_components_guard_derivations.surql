/**
[env]
imports = ["language/write/id_components_guard_derivations_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:london" }

[test]
reason = """
	A table permission that reads a component of the record id is judged over
	the record with the component derived, for every verb and for the output of
	a write: a record user writes the records of its own city and the rows of
	another city are refused or skipped, whether the permission guards the write
	or only what it returns. A write the permission allows but whose output it
	hides still happens, which the event it fires shows with the components it
	read.
"""

# The create permission reads the component of the record being made.
[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-02T00:00:00Z'], temp: 5, time: d'2024-01-02T00:00:00Z' }]"

[[test.results]]
value = "[]"

# The update permission reads it on the record as it was and as it is made.
[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 40, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[]"

# The delete permission reads it on the record being removed.
[[test.results]]
value = "[]"

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-02T00:00:00Z'], temp: 5, time: d'2024-01-02T00:00:00Z' }]"

# Only the record the caller may select is left to read.
[[test.results]]
value = "[{ temp: 40 }]"

# The select permission hides the output of a create it did not guard.
[[test.results]]
value = "[]"

[[test.results]]
value = "[{ city: 'London', id: ledger:['London', d'2024-01-01T00:00:00Z'], n: 2, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ n: 2 }]"

# The hidden create happened, and the event read its component.
[[test.results]]
value = "[{ city: 'Paris', n: 1 }, { city: 'London', n: 2 }]"
*/

-- The create permission.
CREATE reading SET city = 'London', time = d'2024-01-02T00:00:00Z', temp = 5 RETURN AFTER;
CREATE reading SET city = 'Paris', time = d'2024-01-02T00:00:00Z', temp = 8 RETURN AFTER;

-- The update permission.
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 40 RETURN AFTER;
UPDATE reading:['Paris', d'2024-01-01T00:00:00Z'] SET temp = 60 RETURN AFTER;

-- The delete permission.
DELETE reading:['Paris', d'2024-01-01T00:00:00Z'] RETURN BEFORE;
DELETE reading:['London', d'2024-01-02T00:00:00Z'] RETURN BEFORE;

SELECT temp FROM reading ORDER BY temp;

-- The select permission on the output of a write nothing else guards.
CREATE ledger SET city = 'Paris', time = d'2024-01-01T00:00:00Z', n = 1 RETURN AFTER;
CREATE ledger SET city = 'London', time = d'2024-01-01T00:00:00Z', n = 2 RETURN AFTER;
SELECT n FROM ledger ORDER BY n;

-- The hidden create happened, and the event read its component.
SELECT city, n FROM witness ORDER BY n;
