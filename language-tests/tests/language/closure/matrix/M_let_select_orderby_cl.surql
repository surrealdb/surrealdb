/**
[test]
reason = "The body is written in `SELECT a, array::len([a].filter(|$e| $e >= $m)) AS k FROM [{ a: 1 }, { a: 10 }] ORDER BY k DESC`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 10, k: 1 }, { a: 1, k: 0 }]"
*/
LET $m = 8;
SELECT a, array::len([a].filter(|$e| $e >= $m)) AS k FROM [{ a: 1 }, { a: 10 }] ORDER BY k DESC;
