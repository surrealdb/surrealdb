/**
[env]
namespace = false
database = false

[test]

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.namespaces.test == 'DEFINE NAMESPACE test'"
*/

DEFINE NAMESPACE test;
INFO FOR ROOT;
