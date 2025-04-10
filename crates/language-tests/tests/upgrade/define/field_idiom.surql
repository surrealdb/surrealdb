
/**
[env]
imports = ["upgrade/define/field_idiom_import.surql"]

[test]
upgrade = true

[[test.results]]
value = '''{ events: {  }, fields: { "`all`[*]": 'DEFINE FIELD `all`[*] ON tab PERMISSIONS FULL', basic: 'DEFINE FIELD basic ON tab PERMISSIONS FULL', "flatten…": 'DEFINE FIELD flatten… ON tab PERMISSIONS FULL', "index[1]": 'DEFINE FIELD index[1] ON tab PERMISSIONS FULL', "member.member": 'DEFINE FIELD member.member ON tab PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }'''
*/
INFO FOR TABLE tab;
