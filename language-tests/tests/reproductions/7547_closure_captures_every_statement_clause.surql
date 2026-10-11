/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Every clause of a nested statement is part of the expression that holds it.
	A closure captures the outer parameters a nested `SELECT` reads in its
	`START` and `TIMEOUT` clauses, and an `UPDATE` or `UPSERT` in its `TIMEOUT`,
	as it does those read in any other clause. A computed field refuses a write
	in a nested `SELECT`'s `START` or `TIMEOUT` as it refuses one anywhere else
	in its body.

	A `TIMEOUT` that evaluates to `NONE` names no bound, inside a closure as
	at the top level.
"""
issue = 7547

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[[1]]"

[[test.results]]
error = "Cannot define field `c1` as `COMPUTED` bodies must be read-only."

[[test.results]]
error = "Cannot define field `c2` as `COMPUTED` bodies must be read-only."

[[test.results]]
value = "[]"
*/
DEFINE TABLE t SCHEMALESS;
CREATE t:1;
LET $st = 2;
LET $to = 1ms;
LET $big = 1d;

-- Captured in START and TIMEOUT.
RETURN [1].map(|$i| array::len((SELECT VALUE n FROM [{n:1},{n:2},{n:3}] START $st)));
RETURN [1].map(|$i| (SELECT VALUE n FROM [{n:1}] TIMEOUT <duration>$to));
RETURN [1].filter(|$i| (SELECT VALUE n FROM [{n:1}] TIMEOUT <duration>$to) == [1]);
RETURN array::map([1], |$i| (SELECT VALUE n FROM [{n:1}] TIMEOUT <duration>$big));
RETURN [1].map(|$i| (UPDATE ONLY t:1 SET n = 1 TIMEOUT <duration>$big).n);
RETURN [1].map(|$i| (UPSERT ONLY t:2 SET n = 2 TIMEOUT <duration>$big).n);

-- An unbound parameter is NONE, which names no bound in either position.
SELECT VALUE n FROM [{n:1}] TIMEOUT $nope;
RETURN [1].map(|$i| (SELECT VALUE n FROM [{n:1}] TIMEOUT $nope));

-- A computed field's body may not write in any clause.
DEFINE FIELD c1 ON t COMPUTED (SELECT VALUE n FROM [{n:1}] START (CREATE ONLY t:9).n);
DEFINE FIELD c2 ON t COMPUTED (SELECT VALUE n FROM [{n:1}] TIMEOUT (CREATE ONLY t:9).d);
SELECT VALUE id FROM t:9;
