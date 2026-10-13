/**
[test]
reason = "The body is written in `RETURN $fs.map(|$f| $f(5))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"
*/
LET $fs = [];
FOR $m IN [3, 8] { LET $fs = array::append($fs, |$e| $e >= $m); };
RETURN $fs.map(|$f| $f(5));
