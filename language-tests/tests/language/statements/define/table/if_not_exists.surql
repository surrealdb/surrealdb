/**
[test]
reason = "Test DEFINE TABLE IF NOT EXISTS does not replace an existing table"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

*/

DEFINE TABLE person COMMENT 'original';
DEFINE TABLE IF NOT EXISTS person COMMENT 'should be ignored';
INFO FOR TABLE person;
