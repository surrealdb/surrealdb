/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "DEFINE TABLE .. AS SELECT whose aggregation the catalog refuses fails with the catalog's error, and defines nothing"

[[test.results]]
error = "Invalid aggregation: the selector `VALUE` clause is not supported on DEFINE TABLE .. AS SELECT .. GROUP .. aggregates"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"
*/
DEFINE TABLE v AS SELECT VALUE count() FROM t GROUP ALL;
INFO FOR DB;
