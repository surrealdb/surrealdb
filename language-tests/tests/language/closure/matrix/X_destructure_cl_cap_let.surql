/**
[test]
reason = "The body is written in `RETURN { a: [1, 5, 10] }.{ b: a.filter(|$e| $e >= $m) }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ b: [10] }"
*/
LET $m = 8;
RETURN { a: [1, 5, 10] }.{ b: a.filter(|$e| $e >= $m) };
