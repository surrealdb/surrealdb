/**
[test]
reason = "The body is written in `RETURN array::map([[1, 5], [10, 15, 20]], |$o| array::filter($o, |$i| $i >= $o[1]))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[[5], [15, 20]]"
*/
RETURN array::map([[1, 5], [10, 15, 20]], |$o| array::filter($o, |$i| $i >= $o[1]));
