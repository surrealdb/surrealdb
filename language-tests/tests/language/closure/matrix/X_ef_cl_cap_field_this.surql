/**
[test]
reason = "The body is written in `RETURN [{ k: 8, v: 10 }, { k: 8, v: 1 }][WHERE array::any([v], |$e| $e >= $this.k)]`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "[{ k: 8, v: 10 }]"
*/
RETURN [{ k: 8, v: 10 }, { k: 8, v: 1 }][WHERE array::any([v], |$e| $e >= $this.k)];
