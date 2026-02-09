/**
[env]
imports = ["upgrade/define/event_import.surql"]

[test]
upgrade = true

[[test.results]]
value = '''{ events: { ev: "DEFINE EVENT ev ON tab WHEN a == 'b' THEN bla COMMENT 'comment'" }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }'''
*/
INFO FOR TABLE tab;

