/**
[test]
reason = "The body is written in `SELECT array::any([a], |$e| $e >= $m) AS big, count() FROM [{ a: 1 }, { a: 10 }] GROUP BY big ORDER BY big`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ big: false, count: 1 }, { big: true, count: 1 }]"
*/
LET $m = 8;
SELECT array::any([a], |$e| $e >= $m) AS big, count() FROM [{ a: 1 }, { a: 10 }] GROUP BY big ORDER BY big;
