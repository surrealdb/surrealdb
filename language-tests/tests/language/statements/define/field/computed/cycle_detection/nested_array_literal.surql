/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Cycle hidden inside array literal arguments to functions"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> b -> a"

*/

DEFINE TABLE t_array SCHEMAFULL;
DEFINE FIELD a ON t_array COMPUTED math::sum([$this.b, 10]);
DEFINE FIELD b ON t_array COMPUTED math::max([a, 0]);
