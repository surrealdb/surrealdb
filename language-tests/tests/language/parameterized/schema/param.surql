/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ name: "DEFINE PARAM $name VALUE 1 COMMENT 'test comment' PERMISSIONS FULL" }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

*/

LET $comment = "test comment";

DEFINE PARAM $name VALUE 1 COMMENT $comment;
(INFO FOR DB).params;

REMOVE PARAM $name;
(INFO FOR DB).params;