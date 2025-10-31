/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { test: "DEFINE TABLE test TYPE NORMAL SCHEMALESS COMMENT 'test' CHANGEFEED 1d PERMISSIONS FOR select, update, delete NONE, FOR create FULL" }, users: {  } }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMAFULL PERMISSIONS NONE' }, users: {  } }"

*/

DEFINE TABLE test;
INFO FOR DB;

ALTER TABLE test
    SCHEMALESS
    PERMISSIONS FOR create FULL
    CHANGEFEED 1d
    COMMENT 'test'
    TYPE NORMAL;
INFO FOR DB;

ALTER TABLE test
    SCHEMAFULL
    PERMISSIONS NONE
    DROP CHANGEFEED
    DROP COMMENT
    TYPE ANY;
INFO FOR DB;
