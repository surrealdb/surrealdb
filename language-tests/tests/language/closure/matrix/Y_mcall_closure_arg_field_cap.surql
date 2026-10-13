/**
[test]
reason = "The body is written in `RETURN array::map([{ a: [1, 5, 10] }], |$o| $o.a.filter(|$e| $e >= 5 AND $e >= $o.a[0]))`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "[[5, 10]]"
*/
RETURN array::map([{ a: [1, 5, 10] }], |$o| $o.a.filter(|$e| $e >= 5 AND $e >= $o.a[0]));
