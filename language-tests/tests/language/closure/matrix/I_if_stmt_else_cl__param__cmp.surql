/**
[test]
reason = "The body is written in `IF false { RETURN 0; } ELSE { RETURN array::filter([1, 5, 10, 15, 20], |$e| $e >= $m); }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
DEFINE PARAM $m VALUE 8;
IF false { RETURN 0; } ELSE { RETURN array::filter([1, 5, 10, 15, 20], |$e| $e >= $m); };
