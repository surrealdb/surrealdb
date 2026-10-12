/**
[env]
namespace = true
database = true
backend = ["rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
	A read at a VERSION does not apply the record-id structure the catalog
	holds now, since the records at the stamp may have been written under
	another one. A record-id literal is read as written: `t:[5.0]` is not read
	as `t:[5]`, at the stamp or now, once the names are gone. And a scan at a
	stamp is not taken to deliver `ORDER BY id`: the rows of a datetime
	component written while the names were defined come back in id order
	after the names are removed.
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
value = "[1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[r:[d'2024-01-01T00:00:00Z'], r:[d'2024-01-01T00:00:00.500Z'], r:[d'2024-01-01T00:00:01Z']]"

[[test.results]]
value = "[r:[d'2024-01-01T00:00:01Z'], r:[d'2024-01-01T00:00:00.500Z'], r:[d'2024-01-01T00:00:00Z']]"

*/

{
	DEFINE FIELD id ON t TYPE [n: int];
	CREATE t:[5] SET v = 1;
	DEFINE FIELD id ON r TYPE [at: datetime];
	CREATE r SET at = d'2024-01-01T00:00:00Z';
	CREATE r SET at = d'2024-01-01T00:00:00.500Z';
	CREATE r SET at = d'2024-01-01T00:00:01Z';
	RETURN 'OK';
};
SLEEP 50ms;
LET $old = time::now();
SLEEP 50ms;
SELECT VALUE v FROM t:[5.0];
DELETE t;
ALTER FIELD id ON t TYPE [int];
SELECT VALUE v FROM t:[5] VERSION $old;
SELECT VALUE v FROM t:[5.0] VERSION $old;
DELETE r;
REMOVE FIELD id ON r;
SELECT VALUE id FROM r ORDER BY id VERSION $old;
SELECT VALUE id FROM r ORDER BY id DESC VERSION $old;
