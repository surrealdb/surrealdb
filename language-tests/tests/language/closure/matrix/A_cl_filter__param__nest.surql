/**
[test]
reason = "Position: the closure of `array::filter`; the body reads a `DEFINE PARAM` through a closure nested in the body (`array::any([E], |$q| $q >= $m)`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
DEFINE PARAM $m VALUE 8;
RETURN array::filter([1, 5, 10, 15, 20], |$e| array::any([$e], |$q| $q >= $m));
