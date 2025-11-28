/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'strict_test', namespace: 'test' }"

[[test.results]]
error = "The table 'foo' does not exist"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
error = "The table 'foo' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/
DEFINE DATABASE strict_test STRICT;
USE DB strict_test;

(||{
	CREATE foo;
})();
INFO FOR DB;
SELECT * FROM foo;


DEFINE TABLE foo;
SELECT * FROM foo;
