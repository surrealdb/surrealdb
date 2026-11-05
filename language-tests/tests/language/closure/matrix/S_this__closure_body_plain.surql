/**
[test]
reason = "The body is written in `RETURN [1, 5, 10, 15, 20].filter(|$e| $this = NONE)`. A closure body reads the row of the frame that invokes it, and a method invoking it in a frame with no row hands it none, so `$this` is `NONE` for every element."

[[test.results]]
value = "[1, 5, 10, 15, 20]"
*/
RETURN [1, 5, 10, 15, 20].filter(|$e| $this = NONE);
