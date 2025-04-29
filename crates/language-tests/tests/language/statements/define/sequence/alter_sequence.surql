/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { seq: 'DEFINE SEQUENCE seq BATCH 1000 START 0 TIMEOUT 5s' }, tables: {  }, users: {  } }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { seq: 'DEFINE SEQUENCE seq BATCH 1000 START 0' }, tables: {  }, users: {  } }'''

*/
DEFINE SEQUENCE seq;
ALTER SEQUENCE seq TIMEOUT 5s;
INFO FOR DB;
ALTER SEQUENCE seq TIMEOUT 0s;
INFO FOR DB;
