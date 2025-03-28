/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'test' does not exist"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"
*/

ALTER TABLE IF EXISTS test COMMENT 'bla';
ALTER TABLE test COMMENT 'bla';
INFO FOR DATABASE;
