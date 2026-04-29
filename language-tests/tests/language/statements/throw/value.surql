/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "An error occurred: found unexpected value: { test: true }"

*/
LET $err = string::concat('found unexpected value: ', {
	test: true
});
THROW $err;
