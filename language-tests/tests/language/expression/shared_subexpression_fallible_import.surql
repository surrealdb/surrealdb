/**
[test]
reason = "Setup for shared_subexpression_fallible: a user function the call is not shared across."
run = false
*/

DEFINE FUNCTION fn::f($x: int) { RETURN $x + 1 };
