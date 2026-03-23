/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Mutual cycle via $this field access: a -> b -> a"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> b -> a"

*/

DEFINE TABLE t_this_mut SCHEMAFULL;
DEFINE FIELD a ON t_this_mut COMPUTED $this.b + 1;
DEFINE FIELD b ON t_this_mut COMPUTED $this.a + 1;
