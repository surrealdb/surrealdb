/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: { test: 'DEFINE PARAM $test VALUE 12345 PERMISSIONS FULL' }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
value = "[12345]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[56789]"

*/

DEFINE PARAM $test VALUE 12345;
INFO FOR DB;
SELECT * FROM $test;
LET $test = 56789;
SELECT * FROM $test;
