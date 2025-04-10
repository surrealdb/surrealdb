/**
[env]
imports = ["upgrade/schema/define_ns_import.surql"]

[test]
upgrade = true

[[test.results]]
match = ''' $result.namespaces.test_namespace == "DEFINE NAMESPACE test_namespace COMMENT 'a test namespace'" '''

*/
INFO FOR ROOT;
