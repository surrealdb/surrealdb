/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Non-cyclic chain a -> b -> c should succeed"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE TABLE t_bare_chain SCHEMAFULL;
DEFINE FIELD c ON t_bare_chain COMPUTED 42;
DEFINE FIELD b ON t_bare_chain COMPUTED c + 1;
DEFINE FIELD a ON t_bare_chain COMPUTED b + c;
