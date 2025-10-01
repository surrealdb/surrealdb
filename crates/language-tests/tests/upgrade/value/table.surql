/**
[env]
imports = ["upgrade/value/table_import.surql"]

[test]
upgrade = true
wip = true

[[test.results]]
value = "{ events: {  }, fields: { f: 'DEFINE FIELD f ON t VALUE COMPUTED { a } PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

*/
INFO FOR TABLE t;
