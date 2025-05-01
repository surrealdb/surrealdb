/**
[env]
imports = ["upgrade/define/function_import.surql"]

[test]
upgrade = true

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: { test_function_1: "DEFINE FUNCTION fn::test_function_1($a: number, $b: array<int>) {} COMMENT 'A function comment' PERMISSIONS NONE", test_function_2: "DEFINE FUNCTION fn::test_function_2($a: any, $b: object) {} COMMENT 'A function comment' PERMISSIONS WHERE true" }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }'''
*/
INFO FOR DB;
