/**
[test]
reason = "Setup for 3.00.00_function_overwrite.surql: the old binary writes functions whose argument and return types it stores as a `Kind`."
run = false
*/
DEFINE FUNCTION fn::greet($name: string) -> string { RETURN 'hello ' + $name; } COMMENT 'greeting';
DEFINE FUNCTION fn::combine($a: int, $b: int) -> int { RETURN $a + $b; };
DEFINE FUNCTION fn::kept($v: option<int>) -> int { RETURN $v ?? 0; };
