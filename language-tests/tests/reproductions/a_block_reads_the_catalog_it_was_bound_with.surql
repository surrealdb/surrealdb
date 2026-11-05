/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-eval-query = true

[test]
reason = """
	A statement inside a block can change the catalog without the plan being
	able to see it: the change happens inside a body the plan does not hold —
	`eval::surql`, declared `OPAQUE_BODY | WRITES_DATA`, or a user-defined
	function. A later statement in the same block reads the definition that
	change leaves, because it is planned when the block reaches it: the read
	inside the block answers as the identical read after the block does.
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
