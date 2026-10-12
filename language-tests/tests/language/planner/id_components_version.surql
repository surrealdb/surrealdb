/**
[env]
namespace = true
database = true
# The TiKV backend does not support versioned reads, and surrealkv / rocksdb
# refuse them unless the datastore is opened versioned.
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
	A read at a VERSION stamp of a table whose record ids name their
	components plans no key range: the structure was checked against the keys
	present when it was defined, not against those a past stamp holds. The
	table is scanned at the stamp, and the rows are those of the stamp; the
	same read without a stamp reads the key range and the present rows.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
match = '''string::contains($result, "TableScan") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[2]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[3, 10, 20]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND !string::contains($result, "TableScan")'''
error = false
*/

{
	DEFINE TABLE reading SCHEMALESS;
	DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
	CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00.500Z', temp = 1;
	CREATE reading SET city = 'London', time = d'2024-01-01T00:00:01Z', temp = 2;
	RETURN 'OK';
};
SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;
UPDATE reading SET temp = temp * 10 WHERE city = 'London' RETURN NONE;
CREATE reading SET city = 'London', time = d'2024-01-01T00:00:02Z', temp = 3 RETURN NONE;
EXPLAIN SELECT temp FROM reading WHERE city = 'London' VERSION $t1;
SELECT VALUE temp FROM reading WHERE city = 'London' AND time > d'2024-01-01T00:00:00.700Z' ORDER BY temp VERSION $t1;
SELECT VALUE temp FROM reading WHERE city = 'London' ORDER BY temp VERSION $t1;
SELECT VALUE temp FROM reading WHERE city = 'London' ORDER BY temp;
EXPLAIN SELECT temp FROM reading WHERE city = 'London';
