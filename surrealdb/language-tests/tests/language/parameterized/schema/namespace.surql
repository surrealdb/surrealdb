/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ other: "DEFINE NAMESPACE other COMMENT 'other comment'", test: 'DEFINE NAMESPACE test' }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ test: 'DEFINE NAMESPACE test' }'''

*/
LET $namespace = "other";
LET $comment = "other comment";

DEFINE NAMESPACE $namespace COMMENT $comment;

(INFO FOR KV).namespaces;

REMOVE NAMESPACE $namespace;

(INFO FOR KV).namespaces;