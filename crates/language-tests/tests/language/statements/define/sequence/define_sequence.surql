/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { seq1: 'DEFINE SEQUENCE seq1 BATCH 1000', seq2: 'DEFINE SEQUENCE seq2 BATCH 100' }, tables: {  }, users: {  } }'''

[[test.results]]
value = "NONE"

[[test.results]]
error = "The sequence 'seq2' already exists"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { seq1: 'DEFINE SEQUENCE seq1 BATCH 1000', seq2: 'DEFINE SEQUENCE seq2 BATCH 250' }, tables: {  }, users: {  } }'''

*/
DEFINE SEQUENCE seq1;
DEFINE SEQUENCE seq2 BATCH 100;
INFO FOR DB;
DEFINE SEQUENCE OVERWRITE seq2 BATCH 250;
DEFINE SEQUENCE seq2 BATCH 150;
INFO FOR DB;
