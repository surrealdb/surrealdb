/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Self-referential via $this: a depends on itself"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> a"

*/

DEFINE TABLE t_this_self SCHEMAFULL;
DEFINE FIELD a ON t_this_self COMPUTED $this.a * 2;
