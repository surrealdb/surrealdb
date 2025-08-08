/**
[env]
namespace = "foo"
database = "foo"
clean = true

[test]

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
INFO FOR NAMESPACE;
USE NS TEST;
INFO FOR NAMESPACE;
