/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Self-referential computed field: a depends on itself"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among field clauses: a -> a"

*/

DEFINE TABLE t_bare_self SCHEMAFULL;
DEFINE FIELD a ON t_bare_self COMPUTED a * 2;
