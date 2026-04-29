/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Cycle buried inside IF/ELSE branches"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> b -> a"

*/

DEFINE TABLE t_ifelse SCHEMAFULL;
DEFINE FIELD a ON t_ifelse COMPUTED IF $self.b > 0 THEN $self.b ELSE 0 END;
DEFINE FIELD b ON t_ifelse COMPUTED IF a > 10 THEN a ELSE 1 END;
