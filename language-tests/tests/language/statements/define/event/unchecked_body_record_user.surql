/**
[env]
namespace = true
database = true
imports = ["language/statements/define/event/unchecked_body_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:a" }

[test]
reason = """
	An event's WHEN and THEN clauses run with permissions off: a record user
	who fires the event cannot read the secret table, but the clauses read all
	of it, whether the body is a block or an expression.
"""

[[test.results]]
value = "[{ id: src:1 }]"

[[test.results]]
value = "[{ id: log:block, seen: [1, 2] }, { id: log:expr, seen: [1, 2] }, { id: log:when, seen: true }]"

[[test.results]]
value = "[]"
*/

CREATE src:1;
SELECT * FROM log ORDER BY id;
SELECT * FROM secret;
