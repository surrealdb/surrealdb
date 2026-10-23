/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
A closure invoked by a statement read at a VERSION stamp reads at that stamp:
a literal the statement makes, a closure an earlier statement made and the
statement is handed, and one handed to a builtin, whose bodies read a table.
The same closures read the current state where no stamp is named.
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
value = "[{ a: 2, id: t:1 }]"

[[test.results]]
value = "[{ a: 20, id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 10]"

[[test.results]]
value = "[1, 10]"

[[test.results]]
value = "[[1], [10]]"

[[test.results]]
value = "[1, 10]"

[[test.results]]
value = "[[1, 1], [10, 10]]"

[[test.results]]
value = "[2, 20]"

[[test.results]]
value = "[2, 20]"
*/

{
	DEFINE TABLE t SCHEMALESS CHANGEFEED 1h;
	CREATE t:1 SET a = 1;
	CREATE t:2 SET a = 10;
	RETURN 'OK';
};
SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;
UPDATE t:1 SET a = 2;
UPDATE t:2 SET a = 20;
LET $f = |$id| (SELECT VALUE a FROM ONLY $id);
LET $all = || (SELECT VALUE a FROM t ORDER BY a);
SELECT VALUE (|$id| (SELECT VALUE a FROM ONLY $id))(id) FROM t ORDER BY id VERSION $t1;
SELECT VALUE $f(id) FROM t ORDER BY id VERSION $t1;
SELECT VALUE array::map([id], $f) FROM t ORDER BY id VERSION $t1;
SELECT VALUE $all() FROM ONLY t:1 VERSION $t1;
SELECT VALUE [a, $f(id)] FROM t ORDER BY id VERSION $t1;
SELECT VALUE $f(id) FROM t ORDER BY id;
RETURN $all();
