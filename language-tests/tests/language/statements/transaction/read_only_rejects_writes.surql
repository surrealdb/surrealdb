/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A write inside BEGIN READ ONLY fails whether or not the syntactic read-only check can see it, and leaves nothing behind."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:one, value: 1 }]"

[[test.results]]
value = "{ age: 0ns, position: NONE, stale: false }"

[[test.results]]
error = "Invalid statement: Cannot run CREATE on a read only transaction"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "{ age: 0ns, position: NONE, stale: false }"

[[test.results]]
error = "Invalid statement: Cannot run UPDATE on a read only transaction"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "{ age: 0ns, position: NONE, stale: false }"

[[test.results]]
error = "There was a problem with the key-value store: Couldn't write to a read only transaction"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[1]"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { item: 'DEFINE TABLE item TYPE ANY SCHEMALESS PERMISSIONS NONE' }, types: {  }, users: {  } }"
*/
DEFINE TABLE item SCHEMALESS;
CREATE item:one SET value = 1;

BEGIN READ ONLY;
CREATE item:two SET value = 2;
COMMIT;

BEGIN READ ONLY MAX STALENESS 5s;
UPDATE item:one SET value = 9;
COMMIT;

BEGIN READ ONLY;
DEFINE TABLE other;
COMMIT;

SELECT VALUE value FROM item;
INFO FOR DB;
