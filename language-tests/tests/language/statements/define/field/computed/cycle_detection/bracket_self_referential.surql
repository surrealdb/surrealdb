/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Self-referential via bracket access $self[\"a\"]: a -> a"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> a"

*/

DEFINE TABLE t_brack_self SCHEMAFULL;
DEFINE FIELD a ON t_brack_self COMPUTED $self["a"] * 2;
