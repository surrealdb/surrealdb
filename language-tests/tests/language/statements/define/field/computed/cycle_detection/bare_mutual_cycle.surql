/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Mutual cycle via bare field access: a -> b -> a"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> b -> a"

*/

DEFINE TABLE t_bare_mut SCHEMAFULL;
DEFINE FIELD a ON t_bare_mut COMPUTED b + 1;
DEFINE FIELD b ON t_bare_mut COMPUTED a + 1;
