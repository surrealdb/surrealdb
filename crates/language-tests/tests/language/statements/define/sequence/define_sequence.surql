/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { seq1: 'DEFINE SEQUENCE seq1 BATCH 1000 START 0', seq2: 'DEFINE SEQUENCE seq2 BATCH 100 START 0', seq3: 'DEFINE SEQUENCE seq3 BATCH 1000 START 0 TIMEOUT 5s' }, tables: {  }, users: {  } }'''

[[test.results]]
value = "NONE"

[[test.results]]
error = "The sequence 'seq2' already exists"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { seq1: 'DEFINE SEQUENCE seq1 BATCH 1000 START 0', seq2: 'DEFINE SEQUENCE seq2 BATCH 250 START -25', seq3: 'DEFINE SEQUENCE seq3 BATCH 1000 START 0 TIMEOUT 5s' }, tables: {  }, users: {  } }'''

[[test.results]]
value = "{ accesses: [], analyzers: [], apis: [], buckets: [], configs: [], functions: [], models: [], params: [], sequences: [{ batch: '1000', name: 'seq1', start: '0', timeout: NONE }, { batch: '250', name: 'seq2', start: '-25', timeout: NONE }, { batch: '1000', name: 'seq3', start: '0', timeout: 5s }], tables: [], users: [] }"

*/
DEFINE SEQUENCE seq1;
DEFINE SEQUENCE seq2 BATCH 100;
DEFINE SEQUENCE seq3 TIMEOUT 5s;
INFO FOR DB;
DEFINE SEQUENCE OVERWRITE seq2 BATCH 250 START -25;
DEFINE SEQUENCE seq2 BATCH 150;
INFO FOR DB;
INFO FOR DB STRUCTURE;
