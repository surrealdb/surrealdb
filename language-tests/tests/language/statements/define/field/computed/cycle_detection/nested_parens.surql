/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Cycle through deeply nested parenthesized expressions"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> b -> a"

*/

DEFINE TABLE t_parens SCHEMAFULL;
DEFINE FIELD a ON t_parens COMPUTED (1 + ($self.b * 2));
DEFINE FIELD b ON t_parens COMPUTED ((a - 1) / 2);
