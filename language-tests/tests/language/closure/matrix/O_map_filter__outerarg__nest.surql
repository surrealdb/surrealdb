/**
[test]
reason = "The body is written in `RETURN array::map([[1, 5], [10, 15, 20]], |$o| array::filter($o, |$i| array::any([$i], |$q| $q >= $o)))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[[], []]"
*/
RETURN array::map([[1, 5], [10, 15, 20]], |$o| array::filter($o, |$i| array::any([$i], |$q| $q >= $o)));
