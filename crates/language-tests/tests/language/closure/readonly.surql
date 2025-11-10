/**
[test]

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
(||{
	CREATE foo;
})();
INFO FOR DB;
SELECT * FROM foo;


DEFINE TABLE foo;
SELECT * FROM foo;
