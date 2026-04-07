/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Non-cyclic $this on a stored field should succeed"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE TABLE t_stored SCHEMAFULL;
DEFINE FIELD a ON t_stored TYPE number DEFAULT 1;
DEFINE FIELD b ON t_stored COMPUTED $this.a * 2;
