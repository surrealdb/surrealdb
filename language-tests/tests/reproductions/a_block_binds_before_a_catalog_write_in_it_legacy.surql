/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["compute-only"]

[env.capabilities]
allow-functions = true
allow-eval-query = true
allow-arbitrary-query = true

[test]
reason = """
	A divergence, pinned in a pair rather than fixed: the two engines disagree
	on what a statement in a block sees after an earlier statement in that same
	block changed the catalog through a body the planner cannot read.

	The per-row engine resolves each statement as it reaches it, so the
	`SELECT` reads the definition the `eval::surql` before it wrote. The
	streaming engine binds the whole block once, against the catalog as it
	stood before the block ran, so the `SELECT` carries the layer for the
	definition that has since been overwritten and answers with the old
	value. It is right again on the next statement, which is bound after the
	block returned.

	Closing this needs the later statements of a block to be bound at their
	execution boundary, or a catalog write inside a block to force what
	follows it to be bound again — binding, not planning, is where the layer
	is fixed, so neither is a change the fold can make. Until then the
	streaming answer is recorded as it is.
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

-- The next statement is bound after the block, and reads the new definition
-- on either engine.
SELECT VALUE c FROM t;

-- The same holds where the write is one step further away, inside a
-- user-defined function.
DEFINE FUNCTION fn::redefine() { RETURN eval::surql("DEFINE FIELD OVERWRITE c ON t COMPUTED 3") };
{ fn::redefine(); RETURN SELECT VALUE c FROM t };
SELECT VALUE c FROM t;
