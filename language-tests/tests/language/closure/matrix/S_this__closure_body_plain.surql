/**
[test]
reason = "The body is written in `RETURN [1, 5, 10, 15, 20].filter(|$e| $this = NONE)`. A closure that is a method argument reads the method receiver as its current value in a frame with no row."

[[test.results]]
value = "[]"
*/
RETURN [1, 5, 10, 15, 20].filter(|$e| $this = NONE);
