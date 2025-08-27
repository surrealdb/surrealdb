/**
[env]
imports = ["reproductions/5945_create_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
wip = true

[[test.results]]
value = "'temporary value'"
*/

$auth;
$token;
CREATE something;
