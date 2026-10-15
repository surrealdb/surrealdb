
/**
[env]
imports = ["upgrade/define/3.00.00_param_import.surql"]

[test]
upgrade = true

[[test.results]]
value = '''{ comment: "DEFINE PARAM $comment VALUE NONE COMMENT 'comment' PERMISSIONS FULL", permissions_full: 'DEFINE PARAM $permissions_full VALUE NONE PERMISSIONS FULL', permissions_none: 'DEFINE PARAM $permissions_none VALUE NONE PERMISSIONS NONE', permissions_specifics: 'DEFINE PARAM $permissions_specifics VALUE NONE PERMISSIONS WHERE true', value: 'DEFINE PARAM $value VALUE 1 PERMISSIONS FULL' }'''


*/
(INFO FOR DB).params;
