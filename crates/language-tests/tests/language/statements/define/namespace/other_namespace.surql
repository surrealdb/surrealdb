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
value = "{ accesses: {  }, databases: {  }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, databases: {  }, users: {  } }"
*/


DEFINE NAMESPACE test;
USE NAMESPACE foo;
INFO FOR NAMESPACE;
USE NS TEST;
INFO FOR NAMESPACE;
