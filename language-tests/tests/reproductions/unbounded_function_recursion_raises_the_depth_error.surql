/**
[test]
reason = "A function that calls itself, or a pair that call each other, with no base case spends the computation-depth budget and ends in the computation-depth error."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

*/

DEFINE FUNCTION fn::loop() { RETURN fn::loop(); };
fn::loop();

DEFINE FUNCTION fn::ping($n: int) { RETURN fn::pong($n + 1); };
DEFINE FUNCTION fn::pong($n: int) { RETURN fn::ping($n + 1); };
fn::ping(0);
