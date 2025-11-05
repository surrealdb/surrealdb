/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ test: "DEFINE EVENT test ON test WHEN true THEN 'hello world' COMMENT 'test comment'" }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

*/
LET $name = "test";
LET $table = "test";
LET $comment = "test comment";

DEFINE EVENT $name ON $table WHEN true THEN "hello world" COMMENT $comment;

(INFO FOR TABLE $table).events;

REMOVE EVENT $name ON $table;

(INFO FOR TABLE $table).events;