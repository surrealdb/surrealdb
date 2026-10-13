/**
[test]
reason = "Setup for 3.00.00_closure_capture_in_stored_clauses.surql: the old binary writes field and event clauses whose closures read a database param, `$value` and `$after`."
run = false

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE PARAM $m VALUE 8;
DEFINE FIELD f ON w VALUE [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= $m)];
DEFINE FIELD g ON w VALUE [1, 5, 10][WHERE array::any([$this], |$e| $e >= $value)];
DEFINE EVENT ev ON w WHEN true THEN { UPSERT log:1 SET v = [1, 5, 10][WHERE array::any([$this], |$e| $e >= $after.lim)] };
