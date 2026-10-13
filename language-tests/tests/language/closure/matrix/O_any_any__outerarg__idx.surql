/**
[test]
reason = "The body is written in `RETURN array::map([1, 5, 10, 15, 20], |$o| array::any([1, 5, 10], |$i| $i >= [0, 8, 100][$o]))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[true, true, true, true, true]"
*/
RETURN array::map([1, 5, 10, 15, 20], |$o| array::any([1, 5, 10], |$i| $i >= [0, 8, 100][$o]));
