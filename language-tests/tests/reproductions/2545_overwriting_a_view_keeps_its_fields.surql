/**
[env]
timeout = 5000

[test]
reason = "Pins current behaviour, which the issue reports as a defect: DEFINE TABLE OVERWRITE of a view clears the view's field definitions (a plain table's OVERWRITE keeps them), because overwriting a view rebuilds it from nothing. Open question for the owner, not a decided contract."
issue = 2545

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { a: "DEFINE FIELD a ON x VALUE 'a' PERMISSIONS FULL" }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { a: "DEFINE FIELD a ON p VALUE 'a' PERMISSIONS FULL" }, indexes: {  }, lives: {  }, tables: {  } }"""

*/

DEFINE TABLE y;
DEFINE TABLE x AS SELECT * FROM y;
DEFINE FIELD a ON x VALUE 'a';
INFO FOR TABLE x;
DEFINE TABLE OVERWRITE x AS SELECT * FROM y;
INFO FOR TABLE x;
DEFINE TABLE p;
DEFINE FIELD a ON p VALUE 'a';
DEFINE TABLE OVERWRITE p;
INFO FOR TABLE p;
