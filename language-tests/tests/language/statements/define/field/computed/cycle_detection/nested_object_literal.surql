/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Cycle through $this.field inside an object literal value"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> b -> a"

*/

DEFINE TABLE t_obj SCHEMAFULL;
DEFINE FIELD a ON t_obj COMPUTED { val: $this.b }.val;
DEFINE FIELD b ON t_obj COMPUTED { val: a }.val;
