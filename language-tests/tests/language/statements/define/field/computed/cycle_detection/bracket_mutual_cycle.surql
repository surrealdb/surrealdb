/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Mutual cycle via bracket access $self[\"b\"] and $this[\"a\"]: a -> b -> a"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> b -> a"

*/

DEFINE TABLE t_brack_mut SCHEMAFULL;
DEFINE FIELD a ON t_brack_mut COMPUTED $self["b"] + 1;
DEFINE FIELD b ON t_brack_mut COMPUTED $this["a"] + 1;
