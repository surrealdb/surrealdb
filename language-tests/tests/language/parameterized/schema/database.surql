/**
[env]
namespace = "test"

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ test: 'DEFINE DATABASE test', test_database: "DEFINE DATABASE test_database COMMENT 'test comment'" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ test: 'DEFINE DATABASE test' }"

*/
LET $name = "test_database";
LET $comment = "test comment";

DEFINE DATABASE $name COMMENT $comment;

(INFO FOR NS).databases;

REMOVE DATABASE $name;

(INFO FOR NS).databases;
