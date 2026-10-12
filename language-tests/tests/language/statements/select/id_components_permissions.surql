/**
[env]
imports = ["./id_components_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:london" }

[test]
reason = """
	The table's permissions govern the components a record id names: a table
	permission reads them as it reads any field, so a record user sees only the
	rows it admits and nothing of the others, whether the row is scanned,
	named by its id or reached through a record-id path; and the permission of
	a stored field reads them too. The components have no field permissions of
	their own.
"""

[[test.results]]
value = "[{ temp: 4 }, { temp: 5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 4, time: d'2024-01-01T00:00:00Z' }, { city: 'London', id: reading:['London', d'2024-01-02T00:00:00Z'], temp: 5, time: d'2024-01-02T00:00:00Z' }]"

[[test.results]]
value = "[{ n: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'London'"

[[test.results]]
value = "[{ id: gauge:['London', 1], level: 10, seq: 1, site: 'London' }, { id: gauge:['Paris', 1], seq: 1, site: 'Paris' }]"

[[test.results]]
value = "[{ site: 'London' }]"

[[test.results]]
value = "[]"
*/

-- The table permission admits the readings of the caller's city.
SELECT temp FROM reading ORDER BY temp;

-- A filter on a component never reaches a row the permission refuses.
SELECT city, temp FROM reading WHERE city = 'Paris';

-- Naming a refused row by its id yields nothing, and so does reading a
-- component of it through a record-id path.
SELECT city FROM reading:['Paris', d'2024-01-01T00:00:00Z'];
SELECT * FROM reading ORDER BY temp;
SELECT count() AS n FROM reading GROUP ALL;
RETURN reading:['Paris', d'2024-01-01T00:00:00Z'].city;
RETURN reading:['London', d'2024-01-01T00:00:00Z'].city;

-- The permission of a stored field reads a component of the id: the level is
-- cut from the other site's gauge, and a predicate on it sees the cut.
SELECT * FROM gauge ORDER BY site;
SELECT site FROM gauge WHERE level = 10;
SELECT site FROM gauge WHERE level = 20;
