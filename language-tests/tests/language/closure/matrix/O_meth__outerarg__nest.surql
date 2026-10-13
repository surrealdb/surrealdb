/**
[test]
reason = "The body is written in `RETURN [1, 5, 10].map(|$o| [1, 5, 10, 15].filter(|$i| array::any([$i], |$q| $q >= $o)))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[[1, 5, 10, 15], [5, 10, 15], [10, 15]]"
*/
RETURN [1, 5, 10].map(|$o| [1, 5, 10, 15].filter(|$i| array::any([$i], |$q| $q >= $o)));
