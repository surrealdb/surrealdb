/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE FUNCTION fn::test_function_1 ($a: number, $b: array<int>){} COMMENT "A function comment" PERMISSIONS NONE;
DEFINE FUNCTION fn::test_function_2 ($a: any, $b: object){} COMMENT "A function comment" PERMISSIONS WHERE true;
