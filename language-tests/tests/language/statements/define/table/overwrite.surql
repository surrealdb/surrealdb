/**
[test]
reason = "Test DEFINE TABLE OVERWRITE replaces an existing table definition"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

*/

DEFINE TABLE person;
DEFINE TABLE OVERWRITE person COMMENT 'replaced definition';
INFO FOR TABLE person;
