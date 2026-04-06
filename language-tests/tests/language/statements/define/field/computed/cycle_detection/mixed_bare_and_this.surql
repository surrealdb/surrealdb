/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Mixed bare + $this cycle: a uses bare b, b uses $this.a"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> b -> a"

*/

DEFINE TABLE t_mixed SCHEMAFULL;
DEFINE FIELD a ON t_mixed COMPUTED b + 1;
DEFINE FIELD b ON t_mixed COMPUTED $this.a + 1;
