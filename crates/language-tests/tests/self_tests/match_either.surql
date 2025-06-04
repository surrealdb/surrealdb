/**
[test]

[[test.results]]
match = "$result == 'foo' || $error == 'Execution halted with output: `foo`'"
*/

IF rand::bool() {
	'foo';
}else{
	THROW 'foo';
}
