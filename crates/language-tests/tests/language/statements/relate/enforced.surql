/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "The record 'a:1' does not exist"

[[test.results]]
value = "[{ id: a:1 }, { id: a:2 }]"

[[test.results]]
value = "[{ id: edge:1, in: a:1, out: a:2 }]"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { a: 'DEFINE TABLE a TYPE ANY SCHEMALESS PERMISSIONS NONE', edge: 'DEFINE TABLE edge TYPE RELATION ENFORCED SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

*/

DEFINE TABLE edge TYPE RELATION ENFORCED;
RELATE a:1->edge:1->a:2;
CREATE a:1, a:2;
RELATE a:1->edge:1->a:2;
INFO FOR DB;
