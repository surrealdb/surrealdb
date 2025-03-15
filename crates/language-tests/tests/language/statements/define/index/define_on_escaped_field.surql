/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ events: {  }, fields: { "`value`": 'DEFINE FIELD `value` ON test PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }'''

[[test.results]]
value = "NONE"

*/

DEFINE TABLE test SCHEMAFULL;

DEFINE FIELD value ON test;
INFO FOR TABLE test;

DEFINE INDEX idx_field ON test FIELDS value;
