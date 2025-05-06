/**
[env]
imports = ["upgrade/define/field_simple_clauses_import.surql"]

[test]
upgrade = true

[[test.results]]
value = '''{ events: {  }, fields: { "`value`": 'DEFINE FIELD `value` ON tab VALUE 1 PERMISSIONS FULL', assert: 'DEFINE FIELD assert ON tab ASSERT true PERMISSIONS FULL', comment: "DEFINE FIELD comment ON tab COMMENT 'a comment' PERMISSIONS FULL", default: 'DEFINE FIELD default ON tab DEFAULT 1 PERMISSIONS FULL', flexible: 'DEFINE FIELD flexible ON tab FLEXIBLE PERMISSIONS FULL', readonly: 'DEFINE FIELD readonly ON tab READONLY PERMISSIONS FULL', type: 'DEFINE FIELD type ON tab TYPE any PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }'''

*/
INFO FOR TABLE tab;
