/**
[test]
wip = true
reason = "Discovering that a function doesn't exist should be caught during logical planning."

[[test.results]]
value = "NONE"

[[test.results]]
error = "The function 'fn::foo' does not exist"

*/
DEFINE TABLE person;
# This doesn't fail because the person table is empty so the function doesn't get evaluated.
# This should be caught during logical planning.
SELECT fn::foo() FROM person;
