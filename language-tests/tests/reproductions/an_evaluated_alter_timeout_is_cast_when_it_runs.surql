/**
[env]
namespace = true
database = true
auth = { level = "owner" }
remote-exclusive = true

[test]
reason = "An evaluated TIMEOUT of ALTER SEQUENCE and QUERY_TIMEOUT of ALTER SYSTEM is evaluated when the statement runs and cast to a duration"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: { sq: 'DEFINE SEQUENCE sq BATCH 1000 START 0 TIMEOUT 2s' }, tables: {  }, types: {  }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Could not cast into `duration` using input `'abc'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: { sq: 'DEFINE SEQUENCE sq BATCH 1000 START 0' }, tables: {  }, types: {  }, users: {  } }"

[[test.results]]
error = "Could not cast into `duration` using input `'abc'`"

[[test.results]]
error = "Could not cast into `duration` using input `NONE`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
DEFINE SEQUENCE sq;
LET $t = 2s;
ALTER SEQUENCE sq TIMEOUT $t;
INFO FOR DB;
ALTER SEQUENCE sq TIMEOUT (1s + 1s);
ALTER SEQUENCE sq TIMEOUT 'abc';
ALTER SEQUENCE IF EXISTS nope TIMEOUT 'abc';
ALTER SEQUENCE sq TIMEOUT NONE;
INFO FOR DB;
ALTER SYSTEM QUERY_TIMEOUT 'abc';
ALTER SYSTEM QUERY_TIMEOUT NONE;
ALTER SYSTEM QUERY_TIMEOUT $t;
ALTER SYSTEM DROP QUERY_TIMEOUT;
