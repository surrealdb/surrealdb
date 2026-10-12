/**
[env]
imports = ["./id_components_import.surql"]

[test]
reason = """
	The components a record id names are read as fields everywhere a field is
	read: `*`, a projection, WHERE, ORDER BY, GROUP BY, `$this`, a record
	source, a subquery, a record-id path and the body of another computed
	field. They are derived from the id, never stored, so a record that does
	not exist has none.
"""

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], tag: 'London-4', temp: 4, time: d'2024-01-01T00:00:00Z' }, { city: 'London', id: reading:['London', d'2024-01-02T00:00:00Z'], tag: 'London-5', temp: 5, time: d'2024-01-02T00:00:00Z' }, { city: 'Paris', id: reading:['Paris', d'2024-01-01T00:00:00Z'], tag: 'Paris-6', temp: 6, time: d'2024-01-01T00:00:00Z' }, { city: 'Paris', id: reading:['Paris', d'2024-01-03T00:00:00Z'], tag: 'Paris-7', temp: 7, time: d'2024-01-03T00:00:00Z' }]"

[[test.results]]
value = "[{ city: 'London', temp: 4, time: d'2024-01-01T00:00:00Z' }, { city: 'London', temp: 5, time: d'2024-01-02T00:00:00Z' }, { city: 'Paris', temp: 6, time: d'2024-01-01T00:00:00Z' }, { city: 'Paris', temp: 7, time: d'2024-01-03T00:00:00Z' }]"

[[test.results]]
value = "[{ temp: 6 }, { temp: 7 }]"

[[test.results]]
value = "[{ temp: 5 }, { temp: 7 }]"

[[test.results]]
value = "[{ temp: 5 }]"

[[test.results]]
value = "[{ temp: 7 }, { temp: 5 }, { temp: 4 }, { temp: 6 }]"

[[test.results]]
value = "[{ city: 'London', n: 2, total: 9 }, { city: 'Paris', n: 2, total: 13 }]"

[[test.results]]
value = "[{ c: 'London' }]"

[[test.results]]
value = "[{ city: 'Paris', time: d'2024-01-03T00:00:00Z' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ city: 'Paris', temp: 6 }, { city: 'Paris', temp: 7 }]"

[[test.results]]
value = "[{ city: 'London', time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "'London'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['London']"

[[test.results]]
value = "[{ tag: 'London-4' }, { tag: 'London-5' }, { tag: 'Paris-6' }, { tag: 'Paris-7' }]"
*/

-- `*` carries the components beside the stored fields and the computed one.
SELECT * FROM reading ORDER BY temp;

-- A projection of components and a stored field.
SELECT city, time, temp FROM reading ORDER BY temp;

-- A component as a WHERE operand: equality on one, a range on the other, and
-- both together.
SELECT temp FROM reading WHERE city = 'Paris' ORDER BY temp;
SELECT temp FROM reading WHERE time >= d'2024-01-02T00:00:00Z' ORDER BY temp;
SELECT temp FROM reading WHERE city = 'London' AND time > d'2024-01-01T00:00:00Z';

-- ORDER BY components that the projection does not select.
SELECT temp FROM reading ORDER BY time DESC, city;

-- A component as a group key.
SELECT city, count() AS n, math::sum(temp) AS total FROM reading GROUP BY city ORDER BY city;

-- The row as `$this`.
SELECT $this.city AS c FROM reading WHERE temp = 4;

-- A record source reads the components of the record it names; a record that
-- does not exist has none.
SELECT city, time FROM reading:['Paris', d'2024-01-03T00:00:00Z'];
SELECT city FROM reading:['Rome', d'2024-01-01T00:00:00Z'];

-- A subquery carries them through `*`, and through its own projection.
SELECT city, temp FROM (SELECT * FROM reading WHERE temp > 5) ORDER BY temp;
SELECT * FROM (SELECT city, time FROM reading WHERE temp = 4);

-- A record-id path reads the record's component; a record that does not exist
-- gives NONE.
RETURN reading:['London', d'2024-01-02T00:00:00Z'].city;
RETURN reading:['Rome', d'2024-01-01T00:00:00Z'].city;
SELECT VALUE city FROM reading:['London', d'2024-01-02T00:00:00Z'];

-- A computed field reads a component, which derives before it.
SELECT tag FROM reading ORDER BY temp;
