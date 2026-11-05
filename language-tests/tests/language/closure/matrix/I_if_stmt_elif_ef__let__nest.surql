/**
[test]
reason = "The body is written in `IF false { RETURN 0; } ELSE IF true { RETURN [1, 5, 10, 15, 20][WHERE array::any([$this], |$q| $q >= $m)]; }`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
LET $m = 8;
IF false { RETURN 0; } ELSE IF true { RETURN [1, 5, 10, 15, 20][WHERE array::any([$this], |$q| $q >= $m)]; };
