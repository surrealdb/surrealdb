/**
[test]

[[test.results]]
error = "The table 'test' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"

*/

ALTER TABLE test COMMENT 'bla';
ALTER TABLE IF EXISTS test COMMENT 'bla';
INFO FOR DB

