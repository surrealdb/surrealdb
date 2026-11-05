
/**
[env]
compatibility = 3
imports = ["upgrade/define/3.00.00_param_import.surql"]

[test]
upgrade = true

# A released binary reports no compatibility level; the current build reads
# the database a released binary defined at level 3.
[[test.results]]
match = '''$result.compatibility IN [NONE, 3]
	&& $result.params == { comment: "DEFINE PARAM $comment VALUE NONE COMMENT 'comment' PERMISSIONS FULL", permissions_full: 'DEFINE PARAM $permissions_full VALUE NONE PERMISSIONS FULL', permissions_none: 'DEFINE PARAM $permissions_none VALUE NONE PERMISSIONS NONE', permissions_specifics: 'DEFINE PARAM $permissions_specifics VALUE NONE PERMISSIONS WHERE true', value: 'DEFINE PARAM $value VALUE 1 PERMISSIONS FULL' }'''


*/
INFO FOR DB;
