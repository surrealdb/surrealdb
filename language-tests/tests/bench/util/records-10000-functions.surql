/**
[env]
imports = ["./records-10000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FUNCTION fn::over_block($x: number) { LET $d = $x * 2; RETURN $d > 150; };
	DEFINE FUNCTION fn::over_expr($x: number) { RETURN $x * 2 > 150; };
	RETURN 'OK';
}
