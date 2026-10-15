/**
[env]
namespace = true
database = true
# The TiKV backend does not support versioned reads, and surrealkv / rocksdb
# refuse them unless the datastore is opened versioned.
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
`INFO FOR DB`, `INFO FOR NS` and `INFO FOR TABLE` read the definitions as they
stood at the `VERSION` timestamp: what was defined after it is absent, in the
text and the structured form alike, sequences and databases included. A
timestamp before everything was defined yields the empty info, and the same
statement without `VERSION` reads the present definitions.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: { f: 'DEFINE FUNCTION fn::f() { RETURN 1 } PERMISSIONS FULL' }, models: {  }, params: { p: 'DEFINE PARAM $p VALUE 1 PERMISSIONS FULL' }, sequences: { s1: 'DEFINE SEQUENCE s1 BATCH 1000 START 0' }, tables: { a: 'DEFINE TABLE a TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: { f: 'DEFINE FUNCTION fn::f() { RETURN 1 } PERMISSIONS FULL', g: 'DEFINE FUNCTION fn::g() { RETURN 2 } PERMISSIONS FULL' }, models: {  }, params: { p: 'DEFINE PARAM $p VALUE 1 PERMISSIONS FULL', q: 'DEFINE PARAM $q VALUE 2 PERMISSIONS FULL' }, sequences: { s1: 'DEFINE SEQUENCE s1 BATCH 1000 START 0', s2: 'DEFINE SEQUENCE s2 BATCH 1000 START 0' }, tables: { a: 'DEFINE TABLE a TYPE ANY SCHEMALESS PERMISSIONS NONE', b: 'DEFINE TABLE b TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "{ accesses: [], analyzers: [], apis: [], buckets: [], compatibility: 4, configs: [], extensions: [], functions: [{ args: [], block: '{ RETURN 1 }', name: 'f', permissions: true }], models: [], params: [{ name: 'p', permissions: true, value: '1' }], sequences: [{ batch: '1000', name: 's1', resume: '0', start: '0', timeout: NONE }], tables: [{ drop: false, id: 0, kind: { kind: 'ANY' }, name: 'a', permissions: { create: false, delete: false, select: false, update: false }, schemafull: false }], users: [] }"

[[test.results]]
value = "{ accesses: {  }, databases: { d1: 'DEFINE DATABASE d1 COMPATIBILITY 4', test: 'DEFINE DATABASE test COMPATIBILITY 4' }, users: {  } }"

[[test.results]]
value = "{ accesses: {  }, databases: { d1: 'DEFINE DATABASE d1 COMPATIBILITY 4', d2: 'DEFINE DATABASE d2 COMPATIBILITY 4', test: 'DEFINE DATABASE test COMPATIBILITY 4' }, users: {  } }"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"
*/
DEFINE TABLE a;
DEFINE FUNCTION fn::f() { RETURN 1 };
DEFINE PARAM $p VALUE 1;
DEFINE SEQUENCE s1;
DEFINE DATABASE d1;
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;
DEFINE TABLE b;
DEFINE FUNCTION fn::g() { RETURN 2 };
DEFINE PARAM $q VALUE 2;
DEFINE SEQUENCE s2;
DEFINE DATABASE d2;
INFO FOR DB VERSION $ts;
INFO FOR DB;
INFO FOR DB VERSION $ts STRUCTURE;
INFO FOR NS VERSION $ts;
INFO FOR NS;
INFO FOR TABLE a VERSION $ts;
INFO FOR TABLE b VERSION $ts;
INFO FOR DB VERSION d'2000-01-01T00:00:00Z';
