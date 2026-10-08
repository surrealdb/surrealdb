/**
# 10,000 records of `record`, each `{ n }`, and two functions: `fn::trivial`,
# a body the planner may inline, and `fn::nontrivial`, a body with a `LET`,
# which a call runs as a unit. `run = false`.

[test]
run = false
*/

DEFINE FUNCTION fn::trivial($x: int) { RETURN $x + 1 };
DEFINE FUNCTION fn::nontrivial($x: int) { LET $y = $x + 1; RETURN $y };
FOR $i IN 0..10000 { CREATE type::record('record', $i) SET n = $i; };
