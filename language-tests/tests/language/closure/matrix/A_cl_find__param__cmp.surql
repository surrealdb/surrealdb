/**
[test]
reason = "Position: the closure of `array::find`; the body reads a `DEFINE PARAM` through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "10"
*/
DEFINE PARAM $m VALUE 8;
RETURN array::find([1, 5, 10, 15, 20], |$e| $e >= $m);
