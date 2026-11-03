/**
[env]
namespace = true
database = true
imports = ["language/types/named/cast_to_unreadable_type_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = "A cast to a type the session may not read is refused, as reading its labels is, so its body and checks are not revealed"

[[test.results]]
error = "You don't have permission to read the type 'hidden'"

[[test.results]]
error = "You don't have permission to read the type 'hidden'"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
value = "'a'"
*/

RETURN <hidden>'a';
RETURN enum::cast('hidden', 'z');
RETURN enum::values('hidden');
RETURN <shown>'a';
