/**
[env]
namespace = false
database = false
strict = true
clean = true

[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "The namespace 'foo' does not exist"

[[test.results]]
error = "Specify a namespace to use"

[[test.results]]
error = "The namespace 'TEST' does not exist"

[[test.results]]
error = "Specify a namespace to use"
*/


DEFINE NAMESPACE test;
USE NAMESPACE foo;
INFO FOR NAMESPACE;
USE NS TEST;
INFO FOR NAMESPACE;
