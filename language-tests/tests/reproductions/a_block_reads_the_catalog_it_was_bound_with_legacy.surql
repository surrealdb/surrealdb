/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["compute-only"]

[env.capabilities]
allow-eval-query = true

[test]
reason = """
	Half of a pair. Same queries as its sibling; the two files' expected
	results are the divergence, and the per-row engine's answer is the correct
	one.

	A statement inside a block can change the catalog without the plan being
	able to see it: the change happens inside a body the plan does not hold —
	`eval::surql`, declared `OPAQUE_BODY | WRITES_DATA`, or a user-defined
	function. A later statement in the same block then reads a definition that
	no longer exists.

	The per-row engine resolves each statement as it reaches it, so the second
	statement of the block reads the field the first one just wrote. The
	streaming planner binds the whole block once, before any of it runs, and
	the layer stack for `t` is derived from the definition of `c` in force at
	that moment — so the read inside the block answers with the old body while
	the identical read after the block answers with the new one.

	Both shapes are here because they fail for the same reason through
	different doors, and the trailing read after each block is what shows the
	staleness is scoped to the block rather than to the session.

	Recorded rather than fixed: making a block a binding boundary after an
	opaque-bodied statement is a design change with a cost, and the top level
	already is one. Read this file next to its pair rather than on its own.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

# Inside the block, after `eval::surql` redefined `c` as `COMPUTED 2`.
[[test.results]]
value = "[2]"

# The identical read, one statement later, outside the block.
[[test.results]]
value = "[2]"

[[test.results]]
value = "NONE"

# Inside the block, after a UDF body redefined `c` as `COMPUTED 3`.
[[test.results]]
value = "[3]"

# The identical read, one statement later, outside the block.
[[test.results]]
value = "[3]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD c ON t COMPUTED 1;
CREATE t:1 RETURN NONE;

-- The catalog changes inside `eval::surql`, which the plan holds as an opaque
-- body, so nothing in the block's own shape says the definition moved.
{
	LET $ignored = eval::surql("DEFINE FIELD OVERWRITE c ON t COMPUTED 2");
	RETURN SELECT VALUE c FROM t;
};
SELECT VALUE c FROM t;

-- The same through a user-defined function body.
DEFINE FUNCTION fn::bump() { DEFINE FIELD OVERWRITE c ON t COMPUTED 3; RETURN 0; };
{
	LET $ignored = fn::bump();
	RETURN SELECT VALUE c FROM t;
};
SELECT VALUE c FROM t;
