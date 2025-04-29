/**
[test]

[[test.results]]
match = "$result == 'foo' || $error == 'An error occurred: foo'"
*/

IF rand::bool() {
	'foo';
}else{
	THROW 'foo';
}
