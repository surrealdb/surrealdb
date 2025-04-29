/**
[test]

[[test.results]]
value = "NONE"

*/

DEFINE FIELD f ON t VALUE <future> {
	// This will be parsed as `Value::table`
	a;
};
