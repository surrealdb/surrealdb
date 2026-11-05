/**
[test]
reason = "The body is written in `IF true { RETURN array::any([1, 5, 10, 15, 20], |$e| $e >= $m); }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"
*/
DEFINE PARAM $m VALUE 8;
IF true { RETURN array::any([1, 5, 10, 15, 20], |$e| $e >= $m); };
