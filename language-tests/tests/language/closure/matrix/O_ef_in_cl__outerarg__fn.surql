/**
[test]
reason = "The body is written in `RETURN array::map([1, 5, 10], |$o| [1, 5, 10, 15][WHERE math::max([$this, $o]) = $this])`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[[1, 5, 10, 15], [5, 10, 15], [10, 15]]"
*/
RETURN array::map([1, 5, 10], |$o| [1, 5, 10, 15][WHERE math::max([$this, $o]) = $this]);
