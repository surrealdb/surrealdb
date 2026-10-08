/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-functions = true
allow-eval-query = true
allow-arbitrary-query = true

[test]
reason = """
	A statement in a block sees the catalog an earlier statement of the same
	block changed through a body the planner cannot read.

	The block runs `eval::surql`, whose body redefines a field, and then
	selects the field: the `SELECT` is planned when the block reaches it, so it
	reads the definition the `eval::surql` wrote, exactly as the identical read
	after the block does. The same holds one step further away, where the write
	is made by a user-defined function.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 1, id: t:1 }]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD c ON t COMPUTED 1;
CREATE t:1;

-- The write is made by a body the planner cannot read.
{ eval::surql("DEFINE FIELD OVERWRITE c ON t COMPUTED 2"); RETURN SELECT VALUE c FROM t };

-- The next statement reads the same definition.
SELECT VALUE c FROM t;

-- The same holds where the write is one step further away, inside a
-- user-defined function.
DEFINE FUNCTION fn::redefine() { RETURN eval::surql("DEFINE FIELD OVERWRITE c ON t COMPUTED 3") };
{ fn::redefine(); RETURN SELECT VALUE c FROM t };
SELECT VALUE c FROM t;
