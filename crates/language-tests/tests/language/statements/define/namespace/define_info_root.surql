/**
[env]
namespace = false
database = false
clean = true

[test]

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.namespaces.test == 'DEFINE NAMESPACE test'"
*/

DEFINE NAMESPACE test;
INFO FOR ROOT;
