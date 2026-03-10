/**
[env]
namespace = false
database = false
clean = true

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: NONE, namespace: 'foo' }"

[[test.results]]
value = "{ accesses: {  }, databases: {  }, users: {  } }"

[[test.results]]
value = "{ database: NONE, namespace: 'TEST' }"

[[test.results]]
value = "{ accesses: {  }, databases: {  }, users: {  } }"
*/


DEFINE NAMESPACE test;
USE NAMESPACE foo;
INFO FOR NAMESPACE;
USE NS TEST;
INFO FOR NAMESPACE;
