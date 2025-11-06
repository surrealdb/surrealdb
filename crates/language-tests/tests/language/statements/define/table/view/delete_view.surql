/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMALESS PERMISSIONS NONE', test_view: 'DEFINE TABLE test_view TYPE ANY SCHEMALESS AS SELECT math::mean(num) AS mean, group FROM test GROUP BY group PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
error = "Invalid query: Cannot delete table `test` on which a view is defined, table(s) `test_view` are defined as a view on this table."

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMALESS PERMISSIONS NONE', test_view: 'DEFINE TABLE test_view TYPE ANY SCHEMALESS AS SELECT math::mean(num) AS mean, group FROM test GROUP BY group PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

*/

DEFINE TABLE test;

CREATE |test:10| CONTENT {
	num: rand::float(),
	group: rand::enum(1,2,3,4,5)
} RETURN NONE;

DEFINE TABLE test_view AS
	SELECT math::mean(num) as mean, group FROM test GROUP BY group;

INFO FOR DB;

REMOVE TABLE test;
INFO FOR DB;
REMOVE TABLE test_view;
INFO FOR DB;
REMOVE TABLE test;
INFO FOR DB;
