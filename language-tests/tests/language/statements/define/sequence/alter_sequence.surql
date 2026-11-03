/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: { seq: 'DEFINE SEQUENCE seq BATCH 1000 START 0 TIMEOUT 5s' }, tables: {  }, types: {  }, users: {  } }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: { seq: 'DEFINE SEQUENCE seq BATCH 1000 START 0' }, tables: {  }, types: {  }, users: {  } }'''

*/
DEFINE SEQUENCE seq;
ALTER SEQUENCE seq TIMEOUT 5s;
INFO FOR DB;
ALTER SEQUENCE seq TIMEOUT NONE;
INFO FOR DB;
