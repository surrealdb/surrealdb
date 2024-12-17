/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: { test: 'DEFINE TABLE test TYPE NORMAL SCHEMAFULL PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: { test: 'DEFINE TABLE test TYPE NORMAL SCHEMAFULL PERMISSIONS NONE' }, users: {  } }"

*/
DEFINE TABLE test SCHEMAFUL;
INFO FOR DB;

REMOVE TABLE test;
DEFINE TABLE test SCHEMAFULL;
INFO FOR DB;
