/**
[env]
imports = ["language/param/path_into_permission_checked_param_import.surql"]
clean = true

signup = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "user@example.com",
}"""

[test]
reason = "A path rooted at a DEFINE PARAM judges the definition's PERMISSIONS on every read, however deep the path; only a binding in the caller's own scope (LET, a block, a closure capture) shadows the definition, and the denial returns once that scope ends."

[[test.results]]
error = "You don't have permission to view the $secret parameter"

[[test.results]]
error = "You don't have permission to view the $secret parameter"

[[test.results]]
error = "You don't have permission to view the $gated parameter"

[[test.results]]
value = "4"

[[test.results]]
value = "[3, 4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "9"

[[test.results]]
value = "[9]"

[[test.results]]
value = "8"

[[test.results]]
error = "You don't have permission to view the $gated parameter"
*/
$secret.a[0];
$secret[0];
$gated.a[1];
$open.a[1];
$open.a;

LET $secret = { a: [9] };
$secret.a[0];
array::map([0], |$i| $secret.a[$i]);
{
	LET $gated = [8];
	$gated[0];
};
$gated.a;
