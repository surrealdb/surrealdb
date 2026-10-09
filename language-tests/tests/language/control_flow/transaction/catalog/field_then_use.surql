/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A statement in a later transaction sees every definition committed before
	that transaction opened: those of the request's own earlier statements.
	Here a field's VALUE, DEFAULT, COMPUTED and ASSERT clauses apply to the
	records created after the definition.
"""

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
value = "[{ a: 1, c: 10, d: 'none', id: t:1, n: 1, v: 2 }]"

[[test.results]]
value = "[{ a: 1, c: 10, d: 'none', id: t:1, n: 1, v: 2 }]"

[[test.results]]
error = "Found 0 for field `n`, with record `t:2`, but field must conform to: $value > 0"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD v ON t VALUE a + 1;
DEFINE FIELD d ON t DEFAULT 'none';
DEFINE FIELD c ON t COMPUTED a * 10;
DEFINE FIELD n ON t ASSERT $value > 0;

CREATE t:1 SET a = 1, n = 1;
SELECT * FROM t:1;
CREATE t:2 SET a = 2, n = 0;
