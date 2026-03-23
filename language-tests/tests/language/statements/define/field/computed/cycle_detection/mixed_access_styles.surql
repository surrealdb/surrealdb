/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Cycle with bare, $this, and $self[\"bracket\"] access in a single expression"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> b -> a"

*/

DEFINE TABLE t_mix_style SCHEMAFULL;
DEFINE FIELD a ON t_mix_style COMPUTED (b + $this.b + $self["b"]) / 3;
DEFINE FIELD b ON t_mix_style COMPUTED $self.a + 1;
