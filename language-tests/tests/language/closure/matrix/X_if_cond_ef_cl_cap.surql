/**
[test]
reason = "The body is written in `IF [10][WHERE array::any([$this], |$e| $e >= $m)] != [] { RETURN 'y'; } ELSE { RETURN 'n'; }`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "'y'"
*/
LET $m = 8;
IF [10][WHERE array::any([$this], |$e| $e >= $m)] != [] { RETURN 'y'; } ELSE { RETURN 'n'; };
