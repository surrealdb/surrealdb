/**
[env]
namespace = false
database = false
clean = true

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The namespace 'TEST' does not exist"
*/


DEFINE NAMESPACE test;
USE NS TEST;
INFO FOR NAMESPACE;
