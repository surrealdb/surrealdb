/**
[env]
auth = { level = "editor" }
imports = ["language/permissions/ddl/info_editor_allowed_import.surql"]

[test]
reason = "Editor can execute INFO FOR NAMESPACE, INFO FOR DATABASE, and INFO FOR TABLE"

[[test.results]]
value = "{ accesses: {  }, databases: { test: 'DEFINE DATABASE test' }, users: {  } }"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: { info_tbl: 'DEFINE TABLE info_tbl TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

*/
INFO FOR NAMESPACE;
INFO FOR DATABASE;
INFO FOR TABLE info_tbl;
