/**
[test]
reason = "The body is written in `RETURN array::map([{ a: [1, 5, 10], k: 5 }], |$o| $o.a.filter(|$e| $e >= $o.k))`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "[[5, 10]]"
*/
RETURN array::map([{ a: [1, 5, 10], k: 5 }], |$o| $o.a.filter(|$e| $e >= $o.k));
