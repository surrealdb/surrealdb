/**
[test]
reason = "The body is written in `RETURN array::fold([1, 5], 0, |$a, $e| $a + array::len([1, 5, 10].filter(|$x| $x >= $e)))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "5"
*/
RETURN array::fold([1, 5], 0, |$a, $e| $a + array::len([1, 5, 10].filter(|$x| $x >= $e)));
