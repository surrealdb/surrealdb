/**
[test]
reason = "Position: an object literal that holds a closure and an element filter; the body reads a `LET` of the statement through a closure nested in the body (`array::any([E], |$q| $q >= $m)`). It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: [10, 15, 20], b: [10, 15, 20] }"
*/
LET $m = 8;
RETURN { a: [1, 5, 10, 15, 20].filter(|$e| array::any([$e], |$q| $q >= $m)), b: [1, 5, 10, 15, 20][WHERE array::any([$this], |$q| $q >= $m)] };
