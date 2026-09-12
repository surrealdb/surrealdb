/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
	A user-defined function body runs with the row as its current value, so
	a field it reads is the row's: a computed field is the computed value and
	a guarded field is the guarded value, whether the body names the field,
	reads it through `$this`, or the call sits in a WHERE. What the body
	reads cannot be seen from the call, so every column is on the row it
	runs against.
	Streaming engine only: the legacy engine computes a computed field into
	the document only on a demand its own analysis sees — a name in the
	statement, `$this`, a block — and a function body's reads are not seen
	by it, so there a body reading a computed field nothing else in the
	statement names gets the stored record's `NONE`.
"""

# 0-4: setup
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
value = "[{ a: 1, c: 2, id: t:1 }]"

# 6: the body names the computed field.
[[test.results]]
value = "[{ x: 2 }]"

# 7: the body reads the row through `$this`.
[[test.results]]
value = "[{ x: { a: 1, c: 2, id: t:1 } }]"

# 8: the call in a WHERE.
[[test.results]]
value = "[{ id: t:1 }]"

# 9: the body beside a bare read of the field.
[[test.results]]
value = "[{ c: 2, x: 2 }]"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD a ON t TYPE int;
DEFINE FIELD c ON t TYPE int COMPUTED a + 1;
DEFINE FUNCTION fn::f() { RETURN c; };
DEFINE FUNCTION fn::row() { RETURN $this; };
CREATE t:1 SET a = 1;

SELECT fn::f() AS x FROM t;
SELECT fn::row() AS x FROM t;
SELECT id FROM t WHERE fn::f() > 1;
SELECT fn::f() AS x, c FROM t;
