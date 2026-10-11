/**
[env]
backend = ["rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
	A versioned read in a block, after one of the block's schema statements,
	reads the database as it stood at the stamp its `VERSION` names.

	A block is planned whole before it runs, so each statement after a schema
	statement is planned against the catalog that statement leaves: the schema
	statement runs speculatively into an overlay, and the statements after it
	are bound and mapped over a view of the transaction that reads through the
	overlay. Planning a `VERSION` read converts its datetime into the version
	the storage reads at, and the view has to convert it as the storage beneath
	it does. On SurrealKV a version counts nanoseconds of the engine's own
	commit clock while the change feed's stamps count milliseconds, and a view
	converting in the feed's unit plans the read against a version decades
	before any write: the table's definitions are not found at it.

	The same read is asked outside a block and inside one, after a `DEFINE` of
	an unrelated table, and the pair is the assertion: both see the record as it
	stood at the stamp, with the field computed from it. RocksDB versions its
	reads in the feed's unit, so it is the control.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, c: 10, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 2, c: 20, id: t:1 }]"

[[test.results]]
value = "[{ a: 1, c: 10, id: t:1 }]"

[[test.results]]
value = "[{ a: 1, c: 10, id: t:1 }]"

*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD c ON t COMPUTED a * 10;

CREATE t:1 SET a = 1;
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;
UPDATE t:1 SET a = 2;

SELECT * FROM t VERSION $ts;

{
	DEFINE TABLE other;
	RETURN SELECT * FROM t VERSION $ts;
};
