/**
[test]
reason = "The body is written in `IF array::any([1, 5, 10, 15, 20], |$e| $e >= $m) { RETURN 'y'; } ELSE { RETURN 'n'; }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "'y'"
*/
DEFINE PARAM $m VALUE 8;
IF array::any([1, 5, 10, 15, 20], |$e| $e >= $m) { RETURN 'y'; } ELSE { RETURN 'n'; };
