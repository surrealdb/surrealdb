/**
[test]
reason = "The body is written in `RETURN array::map([1, 2], |$a| array::map([3, 4], |$b| array::map([5, 6], |$c| $c >= [0, 8, 100][$a])))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[[[false, false], [false, false]], [[false, false], [false, false]]]"
*/
RETURN array::map([1, 2], |$a| array::map([3, 4], |$b| array::map([5, 6], |$c| $c >= [0, 8, 100][$a])));
