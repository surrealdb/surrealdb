/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { test: 'DEFINE TABLE test TYPE NORMAL SCHEMAFULL PERMISSIONS NONE', view: 'DEFINE TABLE view TYPE ANY SCHEMALESS AS SELECT count() FROM test GROUP ALL PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: { view: 'DEFINE TABLE view TYPE ANY SCHEMALESS AS SELECT count() FROM test GROUP ALL PERMISSIONS NONE' } }"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: test:awx1vt2p5722v4siq2ny }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: test:v4hybspn2cetvkgt0pl0 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ count: 2, id: view:[] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { test: 'DEFINE TABLE test TYPE NORMAL SCHEMAFULL PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

*/
DEFINE TABLE test SCHEMAFUL;
DEFINE TABLE view AS SELECT count() FROM test GROUP ALL;
INFO FOR DB;
INFO FOR TB test;

SELECT * FROM view;

create test;
create test;

SELECT * FROM view;

REMOVE TABLE view;
INFO FOR DB;
INFO FOR TB test;
