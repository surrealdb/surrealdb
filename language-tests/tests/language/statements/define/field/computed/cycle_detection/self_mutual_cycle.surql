/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Mutual cycle via $self field access: a -> b -> a"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> b -> a"

*/

DEFINE TABLE t_self_mut SCHEMAFULL;
DEFINE FIELD a ON t_self_mut COMPUTED $self.b + 1;
DEFINE FIELD b ON t_self_mut COMPUTED $self.a + 1;
