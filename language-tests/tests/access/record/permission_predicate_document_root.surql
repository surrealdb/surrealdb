/**
[env]
namespace = true
database = true
imports = ["access/record/permission_predicate_document_root_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
issue = 477
reason = "A PERMISSIONS predicate must be able to reach the document root. The legacy engine passes the record as the CursorDoc (doc/check.rs::process_permissions and doc/reduce.rs both pass `Some(doc)`), so `$parent` inside an idiom-level WHERE filter resolves to the row under test. The streaming engine built the predicate's EvalContext with `with_value` alone, leaving `document_root` unset -- and `$parent` is the one reader that does not fall back to `current_value` (physical_expr/literal.rs). Table level: thing:1 has a non-empty `acl` and is alice's, so it survives; thing:2 is bob's; thing:3 is alice's but has an empty `acl`, so the filter yields no elements and the predicate is falsy. Field level: `note` survives only on rows whose `tags` filter matches, which is the same ownership test."

[[test.results]]
value = "user:alice"

[[test.results]]
value = "[{ id: thing:1, note: 'n1' }]"

[[test.results]]
value = "[thing:1]"
*/

RETURN $auth;
SELECT id, note FROM thing ORDER BY id;
SELECT VALUE id FROM thing WHERE owner = user:alice ORDER BY id;
