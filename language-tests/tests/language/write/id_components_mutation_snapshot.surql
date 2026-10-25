/**
[env]
namespace = true
database = true

[test]
reason = """
	The condition and the data clause of an UPDATE or a DELETE read the
	components of the record id as fields, whether the table computes nothing
	else (`reading`, whose components are read from the key) or computes another
	field from them (`mixed`, whose field view is evaluated). Both tables answer
	alike: a data clause can copy a component into a stored field, which the
	records then hold, and a DELETE's earlier image shows the components.
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

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# `reading`: components read from the key.
[[test.results]]
value = "[['London', d'2024-01-01T02:00:00Z'], ['Paris', d'2024-01-01T01:00:00Z']]"

[[test.results]]
value = "[NONE, 'Paris', 'London']"

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 0, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[12]"

[[test.results]]
value = "[{ at: d'2024-01-01T01:00:00Z', copy: 'Paris', id: reading:['Paris', d'2024-01-01T01:00:00Z'], temp: 1 }, { at: d'2024-01-01T02:00:00Z', copy: 'London', id: reading:['London', d'2024-01-01T02:00:00Z'], temp: 12 }]"

# `mixed`: the field view evaluated.
[[test.results]]
value = "[['London', d'2024-01-01T02:00:00Z', 'LONDON'], ['Paris', d'2024-01-01T01:00:00Z', 'PARIS']]"

[[test.results]]
value = "[NONE, 'Paris', 'London']"

[[test.results]]
value = "[{ city: 'London', id: mixed:['London', d'2024-01-01T00:00:00Z'], loud: 'LONDON', temp: 0, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[12]"

[[test.results]]
value = "[{ at: d'2024-01-01T01:00:00Z', copy: 'Paris', id: mixed:['Paris', d'2024-01-01T01:00:00Z'], temp: 1 }, { at: d'2024-01-01T02:00:00Z', copy: 'London', id: mixed:['London', d'2024-01-01T02:00:00Z'], temp: 12 }]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE TABLE mixed SCHEMALESS;
DEFINE FIELD id ON mixed TYPE [city: string, time: datetime];
DEFINE FIELD loud ON mixed COMPUTED string::uppercase(city);
FOR $t IN ['reading', 'mixed'] {
	FOR $i IN 0..3 {
		CREATE type::record($t, [['London', 'Paris', 'London'][$i], time::from_millis(1704067200000 + $i * 3600000)]) SET temp = $i RETURN NONE;
	};
};
LET $from = d'2024-01-01T01:00:00Z';

UPDATE reading SET copy = city, at = time WHERE time >= $from RETURN VALUE [copy, at];
SELECT VALUE copy FROM reading ORDER BY temp;
DELETE reading WHERE city = 'London' AND temp = 0 RETURN BEFORE;
UPDATE reading SET temp += 10 WHERE city != 'Paris' RETURN VALUE temp;
SELECT * OMIT city, time FROM reading ORDER BY temp;

UPDATE mixed SET copy = city, at = time WHERE time >= $from RETURN VALUE [copy, at, loud];
SELECT VALUE copy FROM mixed ORDER BY temp;
DELETE mixed WHERE city = 'London' AND temp = 0 RETURN BEFORE;
UPDATE mixed SET temp += 10 WHERE city != 'Paris' RETURN VALUE temp;
SELECT * OMIT city, time, loud FROM mixed ORDER BY temp;
