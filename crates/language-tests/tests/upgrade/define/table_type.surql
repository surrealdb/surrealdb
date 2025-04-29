/**
[env]
imports = ["upgrade/define/table_type_import.surql"]

[test]
upgrade = true

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { any: 'DEFINE TABLE any TYPE ANY SCHEMALESS PERMISSIONS NONE', normal: 'DEFINE TABLE normal TYPE NORMAL SCHEMALESS PERMISSIONS NONE', relation: 'DEFINE TABLE relation TYPE RELATION IN normal OUT any SCHEMALESS PERMISSIONS NONE', relation_enforced: 'DEFINE TABLE relation_enforced TYPE RELATION IN normal OUT any ENFORCED SCHEMALESS PERMISSIONS NONE', relation_none: 'DEFINE TABLE relation_none TYPE RELATION SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

*/
INFO FOR DB;
