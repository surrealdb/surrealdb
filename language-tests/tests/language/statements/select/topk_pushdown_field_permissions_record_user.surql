/**
[env]
namespace = true
database = true
imports = ["language/statements/select/topk_pushdown_field_permissions_record_user_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
ORDER BY over a field with a conditional select permission, read as a record
user: the sort key is the field as this caller may see it, so a row whose
guard fails sorts as NONE and the TopK threshold cannot be judged from the
stored value. p:1 holds the largest stored `secret` but its guard fails, so
it is last under DESC and first under ASC.
"""

[[test.results]]
value = "user:alice"

[[test.results]]
value = "[p:3, p:2]"

[[test.results]]
value = "[p:1, p:2]"

[[test.results]]
value = "[{ id: p:1, pub: 1 }, { id: p:2, pub: 2, secret: 1 }, { id: p:3, pub: 3, secret: 2 }]"
*/

RETURN $auth;

SELECT VALUE id FROM p ORDER BY secret DESC LIMIT 2;
SELECT VALUE id FROM p ORDER BY secret ASC LIMIT 2;

-- The guard cuts `secret` from p:1 alone.
SELECT * FROM p ORDER BY id;
