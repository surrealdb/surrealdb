/**
[env]
database = false
clean = true

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
value = """{ accesses: {  }, buckets: {  }, databases: { test: 'DEFINE DATABASE test COMPATIBILITY 4', test2: "DEFINE DATABASE test2 COMPATIBILITY 4 COMMENT 'Test'" }, users: {  } }"""

*/

let $comment = NONE;
DEFINE DATABASE test COMMENT $comment;
let $comment = "Test";
DEFINE DATABASE test2 COMMENT $comment;

INFO FOR NS;
