/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A statement in a later transaction sees every definition committed before
	that transaction opened: those of the request's own earlier statements.
	Here a table and a field definition govern the statements that follow
	them.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'b', but no such field exists for table 'x'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, id: u:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 2, b: 7, id: u:2 }]"

[[test.results]]
value = "[{ a: 1, id: u:1 }, { a: 2, b: 7, id: u:2 }]"
*/

DEFINE TABLE x SCHEMAFULL;
DEFINE FIELD a ON x TYPE int;
CREATE x:1 SET a = 1, b = 2;
SELECT * FROM x;

CREATE u:1 SET a = 1;
DEFINE FIELD b ON u VALUE 7;
CREATE u:2 SET a = 2;
SELECT * FROM u ORDER BY id;
