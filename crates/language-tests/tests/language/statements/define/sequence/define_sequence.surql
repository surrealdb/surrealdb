/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { seq1: 'DEFINE SEQUENCE seq1 BATCH 1000 START 0', seq2: 'DEFINE SEQUENCE seq2 BATCH 100 START 0' }, tables: {  }, users: {  } }'''

[[test.results]]
value = "NONE"

[[test.results]]
error = "The sequence 'seq2' already exists"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { seq1: 'DEFINE SEQUENCE seq1 BATCH 1000 START 0', seq2: 'DEFINE SEQUENCE seq2 BATCH 250 START -25' }, tables: {  }, users: {  } }'''

*/
DEFINE SEQUENCE seq1;
DEFINE SEQUENCE seq2 BATCH 100;
INFO FOR DB;
DEFINE SEQUENCE OVERWRITE seq2 BATCH 250 START -25;
DEFINE SEQUENCE seq2 BATCH 150;
INFO FOR DB;
