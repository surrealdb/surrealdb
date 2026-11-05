/**
[test]
reason = "The body is written in `RETURN array::map([1], |$e| (SELECT VALUE $m + $e FROM [1]))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[9]]"
*/
LET $m = 8;
RETURN array::map([1], |$e| (SELECT VALUE $m + $e FROM [1]));
