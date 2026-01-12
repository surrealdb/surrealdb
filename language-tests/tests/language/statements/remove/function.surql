/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The function 'fn::example' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The function 'fn::example' does not exist"

[[test.results]]
value = "NONE"

*/

DEFINE FUNCTION fn::example() {};
DEFINE FUNCTION IF NOT EXISTS fn::example() {};
DEFINE FUNCTION OVERWRITE fn::example() {};
DEFINE FUNCTION fn::example() {};
REMOVE FUNCTION IF EXISTS fn::example();
REMOVE FUNCTION fn::example();
REMOVE FUNCTION IF EXISTS fn::example();
