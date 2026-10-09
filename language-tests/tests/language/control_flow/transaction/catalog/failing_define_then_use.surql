/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A schema statement whose outcome is decided only when it runs, here a
	DEFINE that fails, leaves the catalog as the earlier definitions made it, and
	the statement after it sees that. A failing statement does not stop the
	statements after it, which each open their own transaction.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 't' already exists"

[[test.results]]
value = "[{ id: t:1, x: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The function 'fn::f' already exists"

[[test.results]]
value = "1"

[[test.results]]
value = "[{ a: 1, id: t:2 }]"

[[test.results]]
value = "[{ a: 1, id: t:3 }]"

[[test.results]]
error = "Database index `u` already contains 1, with record `t:2`"

[[test.results]]
value = "[{ a: 1, id: t:4 }]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE TABLE t SCHEMAFULL;
CREATE t:1 SET x = 1;

DEFINE FUNCTION fn::f() { RETURN 1 };
DEFINE FUNCTION fn::f() { RETURN 2 };
RETURN fn::f();

CREATE t:2 SET a = 1;
CREATE t:3 SET a = 1;
DEFINE INDEX u ON t FIELDS a UNIQUE;
CREATE t:4 SET a = 1;
