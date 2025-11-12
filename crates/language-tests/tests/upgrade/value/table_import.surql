/**
[test]

[[test.results]]
value = "NONE"

*/

DEFINE FIELD f ON t COMPUTED {
	a // This will be parsed as `Value::table`
};
