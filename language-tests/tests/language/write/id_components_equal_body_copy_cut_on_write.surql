/**
[env]
namespace = true
database = true

[test]
reason = """
	A record written before its table named the components of its id may store
	a field that holds the value the id holds there. Naming the components is
	accepted, the derived field is what a read shows from then on, and the copy
	is cut the next time the record is written, whichever clause writes it: a
	clause that leaves the field alone, or one that restates it. A record that
	is not written keeps its copy until it is. The stored bodies are read by
	retyping the id to one with no names, which derives nothing.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'London', temp: 1, time: d'2024-01-01T00:00:00Z' }, { city: 'London', temp: 2, time: d'2024-01-02T00:00:00Z' }]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "NONE"

# The written record has lost its copy, and the other has not.
[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-02T00:00:00Z'], temp: 2, time: d'2024-01-02T00:00:00Z' }, { id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 10 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[3]"

[[test.results]]
value = "NONE"

# Now neither has.
[[test.results]]
value = "[{ id: reading:['London', d'2024-01-02T00:00:00Z'], temp: 3 }, { id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 10 }]"
*/

-- Written before the id named its components: the first stores `city`, the
-- second stores both.
CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET city = 'London', temp = 1 RETURN NONE;
CREATE reading:['London', d'2024-01-02T00:00:00Z'] SET city = 'London', time = d'2024-01-02T00:00:00Z', temp = 2 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

-- The derived fields are what is read.
SELECT temp, city, time FROM reading ORDER BY temp;

-- A write that does not name them cuts the copy.
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 10 RETURN VALUE temp;

DEFINE FIELD OVERWRITE id ON reading TYPE [string, datetime];
SELECT * FROM reading ORDER BY temp;

-- A write that restates them cuts the copy too.
DEFINE FIELD OVERWRITE id ON reading TYPE [city: string, time: datetime];
UPDATE reading:['London', d'2024-01-02T00:00:00Z'] CONTENT { city: 'London', time: d'2024-01-02T00:00:00Z', temp: 3 } RETURN VALUE temp;

DEFINE FIELD OVERWRITE id ON reading TYPE [string, datetime];
SELECT * FROM reading ORDER BY temp;
