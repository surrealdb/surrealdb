/**
[test]
reason = "Position: the closure of the `.map` method; the body reads a `LET` of the statement through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[false, false, true, true, true]"
*/
LET $m = 8;
RETURN [1, 5, 10, 15, 20].map(|$e| $e >= $m);
