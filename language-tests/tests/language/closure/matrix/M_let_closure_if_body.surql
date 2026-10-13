/**
[test]
reason = "The body is written in `RETURN array::map([1, 10], |$e| IF $e >= $m { 'hi' } ELSE { 'lo' })`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "['lo', 'hi']"
*/
LET $m = 8;
RETURN array::map([1, 10], |$e| IF $e >= $m { 'hi' } ELSE { 'lo' });
