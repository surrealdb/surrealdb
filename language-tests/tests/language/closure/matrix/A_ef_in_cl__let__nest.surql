/**
[test]
reason = "Position: an element filter inside a closure of `array::map`; the body reads a `LET` of the statement through a closure nested in the body (`array::any([E], |$q| $q >= $m)`). It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[], [10, 15, 20]]"
*/
LET $m = 8;
RETURN array::map([[1, 5], [10, 15, 20]], |$r| $r[WHERE array::any([$this], |$q| $q >= $m)]);
