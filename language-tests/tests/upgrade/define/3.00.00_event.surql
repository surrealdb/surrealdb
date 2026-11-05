/**
[env]
imports = ["upgrade/define/3.00.00_event_import.surql"]

[test]
upgrade = true

[[test.results]]
value = '''{ events: { ev: "DEFINE EVENT ev ON tab WHEN a == 'b' THEN bla COMMENT 'comment'" }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }'''
*/
INFO FOR TABLE tab;

