/**

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { i: 'DEFINE INDEX i ON t COUNT' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: {  }, indexes: { i: "DEFINE INDEX i ON t COUNT COMMENT 'test'" }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { i: 'DEFINE INDEX i ON t COUNT' }, lives: {  }, tables: {  } }"

*/

DEFINE INDEX i ON t COUNT;
INFO FOR TB t;

ALTER INDEX i ON t COMMENT "test";
INFO FOR TB t;

ALTER INDEX i ON t DROP COMMENT;
INFO FOR TB t;