/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "DEFINE PARAM with PERMISSIONS NONE should still be accessible by system-level users (root/ns/db) since PERMISSIONS only applies to record users"

[[test.results]]
value = "NONE"

[[test.results]]
value = "123"

[[test.results]]
value = "123"

[[test.results]]
value = "[123]"

*/

DEFINE PARAM $test VALUE 123 PERMISSIONS NONE;
$test;
RETURN $test;
SELECT * FROM $test;
