/**
[env]
namespace = false
database = false
clean = true

[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "The namespace 'test' already exists"

[[test.results]]
value = "NONE"
*/


DEFINE NAMESPACE test;
DEFINE NAMESPACE test;
DEFINE NAMESPACE IF NOT EXISTS test;
