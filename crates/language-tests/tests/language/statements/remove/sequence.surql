/**
[test]

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
error = "The sequence 'seq2' does not exist"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }'''

*/
DEFINE SEQUENCE seq1;
DEFINE SEQUENCE seq2 BATCH 100;
REMOVE SEQUENCE seq1;
REMOVE SEQUENCE IF EXISTS seq1;
REMOVE SEQUENCE IF EXISTS seq2;
REMOVE SEQUENCE seq2;
INFO FOR DB;
