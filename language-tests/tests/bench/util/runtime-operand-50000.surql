/**
[env]
imports = ["./runtime-operand.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	fn::rt_fill(50000);
	RETURN 'OK';
}
