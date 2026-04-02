/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Cycle hidden inside function arguments: math::abs($this.b) and math::floor(a + 0.5)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> b -> a"

*/

DEFINE TABLE t_func SCHEMAFULL;
DEFINE FIELD a ON t_func COMPUTED math::abs($this.b);
DEFINE FIELD b ON t_func COMPUTED math::floor(a + 0.5);
