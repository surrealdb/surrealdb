/**
[test]
reason = "Position: the closure of `array::map`; the body reads a `DEFINE PARAM` through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[false, false, true, true, true]"
*/
DEFINE PARAM $m VALUE 8;
RETURN array::map([1, 5, 10, 15, 20], |$e| $e >= $m);
