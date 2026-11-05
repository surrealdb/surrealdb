/**
[env]
imports = ["language/statements/define/clause_effects/runtime_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
reason = """
	A schema clause stored with no effect of its own may reach one later,
	through a stored function redefined after the clause: a table's select and
	write permissions, a COMPUTED, VALUE, DEFAULT and ASSERT clause, a field's
	write permission, a function's and a parameter's permission. Each is
	refused where it runs, with the error a definition holding the effect is
	refused with, naming the clause and the construct, and nothing any of them
	reaches is written.
"""

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `CREATE` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `sequence::nextval` advances state outside the transaction"

[[test.results]]
error = "The COMPUTED clause must not have effects: `CREATE` writes data"

[[test.results]]
error = "The VALUE clause must not have effects: `SLEEP` blocks"

[[test.results]]
error = "The DEFAULT clause must not have effects: a schema statement changes the schema"

[[test.results]]
error = "The ASSERT clause must not have effects: `UPSERT` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `INSERT` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `UPSERT` writes data"

[[test.results]]
value = "[]"
*/

SELECT * FROM tsel;
CREATE tupd:1 RETURN NONE;
SELECT c FROM tfld;
CREATE tval:1 RETURN NONE;
CREATE tdef:1 RETURN NONE;
CREATE tass:1 RETURN NONE;
CREATE tfp:1 SET f = 1 RETURN NONE;
RETURN $guarded;
SELECT * FROM log;
