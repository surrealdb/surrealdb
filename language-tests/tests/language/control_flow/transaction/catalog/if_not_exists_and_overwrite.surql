/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A statement in a later transaction sees every definition committed before
	that transaction opened: those of the request's own earlier statements.
	Where the outcome of a schema statement is decided only when it runs, the
	later statement sees the outcome the run produced: IF NOT EXISTS keeps an
	existing definition and creates an absent one, and OVERWRITE replaces an
	existing definition and creates an absent one. Each outcome is covered
	against an absent and against a present definition.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'x', but no such field exists for table 'a'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'x', but no such field exists for table 'a'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'x', but no such field exists for table 'b'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: b:2, x: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: b:3, v: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: b:4, v: 3 }]"
*/

DEFINE TABLE IF NOT EXISTS a SCHEMAFULL;
CREATE a:1 SET x = 1;
DEFINE TABLE IF NOT EXISTS a SCHEMALESS;
CREATE a:2 SET x = 1;

DEFINE TABLE OVERWRITE b SCHEMAFULL;
CREATE b:1 SET x = 1;
DEFINE TABLE OVERWRITE b SCHEMALESS;
CREATE b:2 SET x = 1;

DEFINE FUNCTION IF NOT EXISTS fn::g() { RETURN 1 };
DEFINE FUNCTION IF NOT EXISTS fn::g() { RETURN 2 };
RETURN fn::g();
DEFINE FUNCTION OVERWRITE fn::g() { RETURN 3 };
RETURN fn::g();

DEFINE FIELD IF NOT EXISTS v ON b VALUE 1;
DEFINE FIELD IF NOT EXISTS v ON b VALUE 2;
CREATE b:3;
DEFINE FIELD OVERWRITE v ON b VALUE 3;
CREATE b:4;
