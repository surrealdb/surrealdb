/**
[env]
clean = true
remote-exclusive = true
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "USE selects a namespace and database without checking that they exist, a read of a missing table errors, and strictness is per database (DEFINE DATABASE ... STRICT). INFO FOR DB on a database that does not exist answers with an empty definition"
issue = 2701

[[test.results]]
value = "{ database: 'no_such_db', namespace: 'no_such_ns' }"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, types: {  }, users: {  } }"
*/

USE NS no_such_ns DB no_such_db;
SELECT * FROM missing;
INFO FOR DB;
