/**
[env]
namespace = true
database = true
imports = ["./a_late_bound_read_applies_the_tables_permissions_import.surql", "./a_late_bound_statement_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:probe" }

[test]
reason = """
	A record user's statement planned after a call whose body holds a schema
	statement the plan cannot know the effect of is late-bound:
	it loads its table's select permission, computed fields and field
	permissions when it opens its source. It answers as the same statement
	planned where it stands, each one following it here.
"""

[[test.results]]
value = "[{ a: 1, c: 2, id: t:1, s: 'x' }]"

[[test.results]]
value = "[{ a: 1, c: 2, id: t:1, s: 'x' }]"

[[test.results]]
value = "[{ c: 2, s: 'x' }]"

[[test.results]]
value = "[{ c: 2, s: 'x' }]"

[[test.results]]
value = "[{ id: u:1, k: 'k' }]"

[[test.results]]
value = "[{ id: u:1, k: 'k' }]"

[[test.results]]
value = "[{ a: 3, c: 4, id: t:1, s: 'x' }]"

[[test.results]]
value = "[{ a: 4, c: 5, id: t:1, s: 'x' }]"
*/

{ LET $late = fn::late('now'); RETURN SELECT * FROM t; };
SELECT * FROM t;
{ LET $late = fn::late('now'); RETURN SELECT s, c FROM t; };
SELECT s, c FROM t;
{ LET $late = fn::late('now'); RETURN SELECT * FROM u; };
SELECT * FROM u;
{ LET $late = fn::late('now'); RETURN UPDATE t:1 SET h = 'z', a = 3; };
UPDATE t:1 SET h = 'w', a = 4;
