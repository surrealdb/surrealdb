/**
[test]
reason = "The body is written in `RETURN array::map([1, 2], |$a| array::map([3, 4], |$b| array::map([5, 6], |$c| $c >= $a)))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[[[true, true], [true, true]], [[true, true], [true, true]]]"
*/
RETURN array::map([1, 2], |$a| array::map([3, 4], |$b| array::map([5, 6], |$c| $c >= $a)));
