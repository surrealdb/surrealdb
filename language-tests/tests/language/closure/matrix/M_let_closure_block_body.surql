/**
[test]
reason = "The body is written in `RETURN array::filter([1, 5, 10], |$e| { LET $n = $m; $e >= $n })`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10]"
*/
LET $m = 8;
RETURN array::filter([1, 5, 10], |$e| { LET $n = $m; $e >= $n });
