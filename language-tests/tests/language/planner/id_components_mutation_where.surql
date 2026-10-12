/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An UPDATE or a DELETE over a table whose record ids name their components
	reads its targets through the key ranges its WHERE selects, with a bound a
	parameter holds, and touches exactly the records the WHERE admits, whatever
	it returns. A bound that reads the clock inside a mutation is not folded,
	so it bounds no range; the pinned city still does, and the rows touched
	are right. London holds temps 0 to 10 in even steps every 1.2s, Paris 1 to
	11 in odd ones from 0.6s.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "WriteOperator") AND string::contains($result, "KeyRangeScan") AND string::contains($result, "key: city = 'London'") AND string::contains($result, "range: time < ") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[0, 2, 4]"

[[test.results]]
value = "[0, 2, 4]"

[[test.results]]
value = "[105, 107, 109, 111]"

[[test.results]]
match = '''string::contains($result, "WriteOperator") AND string::contains($result, "KeyRangeScan") AND string::contains($result, "key: city = 'Paris'") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[1, 3]"

[[test.results]]
value = "[0, 2, 4, 6, 8, 10, 105, 107, 109, 111]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "key: city = 'London'") AND !string::contains($result, "range: ")'''
error = false

[[test.results]]
value = "[0, 2, 4, 6, 8, 10]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
FOR $i IN 0..12 {
	CREATE reading SET
		city = ['London', 'Paris'][$i % 2],
		time = time::from_millis(1704067200000 + $i * 600),
		temp = $i
		RETURN NONE;
};
LET $cutoff = d'2024-01-01T00:00:03Z';

EXPLAIN UPDATE reading SET seen = true WHERE city = 'London' AND time < $cutoff;
UPDATE reading SET seen = true WHERE city = 'London' AND time < $cutoff RETURN VALUE temp;
SELECT VALUE temp FROM reading WHERE seen = true ORDER BY temp;
RETURN (UPDATE reading SET temp = temp + 100 WHERE city = 'Paris' AND time >= $cutoff RETURN AFTER).temp;

EXPLAIN DELETE reading WHERE city = 'Paris' AND time < $cutoff;
RETURN (DELETE reading WHERE city = 'Paris' AND time < $cutoff RETURN BEFORE).temp;
SELECT VALUE temp FROM reading ORDER BY temp;

EXPLAIN UPDATE reading SET late = true WHERE city = 'London' AND time < time::now();
UPDATE reading SET late = true WHERE city = 'London' AND time < time::now() RETURN VALUE temp;
