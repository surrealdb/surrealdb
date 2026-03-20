/**
[test]
reason = "Test ALTER PARAM with all variations"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: { test: 'DEFINE PARAM $test VALUE 42 PERMISSIONS FULL' }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: { test: "DEFINE PARAM $test VALUE 100 COMMENT 'updated param' PERMISSIONS FULL" }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: { test: 'DEFINE PARAM $test VALUE 100 PERMISSIONS NONE' }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The param '$nonexistent' does not exist"

*/

-- Setup
DEFINE PARAM $test VALUE 42;
INFO FOR DB;

-- Alter: change value and add comment
ALTER PARAM $test VALUE 100 COMMENT "updated param";
INFO FOR DB;

-- Alter: change permissions and drop comment
ALTER PARAM $test PERMISSIONS NONE DROP COMMENT;
INFO FOR DB;

-- IF EXISTS on non-existent param
ALTER PARAM IF EXISTS $nonexistent VALUE 1;

-- Non-existent param errors
ALTER PARAM $nonexistent VALUE 1;
