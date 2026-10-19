/**
[env]
timeout = 5000

[test]
reason = "DEFINE TABLE OVERWRITE of a view keeps the fields declared on it, as the OVERWRITE of a plain table does"
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
value = """{ events: {  }, fields: { a: "DEFINE FIELD a ON x VALUE 'a' PERMISSIONS FULL" }, indexes: {  }, lives: {  }, tables: {  } }"""

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
