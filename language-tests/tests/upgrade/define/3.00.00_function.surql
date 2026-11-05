/**
[env]
compatibility = 3
imports = ["upgrade/define/3.00.00_function_import.surql"]

[test]
upgrade = true

# A released binary reports no compatibility level; the current build reads
# the database a released binary defined at level 3.
[[test.results]]
match = '''$result.compatibility IN [NONE, 3]
	&& $result.functions == { test_function_1: "DEFINE FUNCTION fn::test_function_1($a: number, $b: array<int>) {;} COMMENT 'A function comment' PERMISSIONS NONE", test_function_2: "DEFINE FUNCTION fn::test_function_2($a: any, $b: object) {;} COMMENT 'A function comment' PERMISSIONS WHERE true" }'''
*/
INFO FOR DB;
