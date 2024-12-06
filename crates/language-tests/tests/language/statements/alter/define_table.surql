/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: { test: "DEFINE TABLE test TYPE NORMAL DROP SCHEMALESS COMMENT 'test' CHANGEFEED 1d PERMISSIONS FOR select, update, delete NONE, FOR create FULL" }, users: {  } }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMAFULL PERMISSIONS NONE' }, users: {  } }"

*/

DEFINE TABLE test;
INFO FOR DB;

ALTER TABLE test
    DROP
    SCHEMALESS
    PERMISSIONS FOR create FULL
    CHANGEFEED 1d
    COMMENT 'test'
    TYPE NORMAL;
INFO FOR DB;

ALTER TABLE test
    DROP false
    SCHEMAFULL
    PERMISSIONS NONE
    CHANGEFEED NONE
    COMMENT NONE
    TYPE ANY;
INFO FOR DB;
