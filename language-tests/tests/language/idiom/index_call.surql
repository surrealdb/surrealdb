/**
[test]
reason = "Test indexing an array with a function call"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"
*/

DEFINE FUNCTION fn::foo() {
	return 1 + 1;
};
RETURN [1,2,3,4][fn::foo()];
