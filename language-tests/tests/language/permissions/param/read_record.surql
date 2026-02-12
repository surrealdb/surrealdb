/**
[env]
imports = ["./import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:test" }

[test]

[[test.results]]
value = "1"

[[test.results]]
error = "You don't have permission to view the $none parameter"
*/

$full;
$none;
