/**
[test]
reason = "Position: a value block that binds a `LET` and runs a closure; the body reads a `LET` of the statement through a closure nested in the body (`array::any([E], |$q| $q >= $m)`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
LET $m = 8;
RETURN { LET $tmp = [1, 5, 10, 15, 20]; $tmp.filter(|$e| array::any([$e], |$q| $q >= $m)) };
