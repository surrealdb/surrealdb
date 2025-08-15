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
error = "The namespace 'foo' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The namespace 'TEST' does not exist"
*/


DEFINE NAMESPACE test;
USE NAMESPACE foo;
INFO FOR NAMESPACE;
USE NS TEST;
INFO FOR NAMESPACE;
