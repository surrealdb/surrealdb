/**
[test]
reason = "The body is written in `RETURN array::fold([1, 5], 0, |$a, $e| array::fold([1, 2], $a, |$b, $x| $b + $x + $a))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "12"
*/
RETURN array::fold([1, 5], 0, |$a, $e| array::fold([1, 2], $a, |$b, $x| $b + $x + $a));
