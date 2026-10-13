/**
[test]
reason = "The body is written in `FOR $m IN [8] { LET $f = |$e| $e >= $m; }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"
*/
LET $f = NONE;
FOR $m IN [8] { LET $f = |$e| $e >= $m; };
RETURN array::filter([1, 5, 10, 15, 20], $f);
