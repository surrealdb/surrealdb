/**
[test]
reason = "The body is written in `RETURN [1, 5, 10][array::len([1, 2].filter(|$e| $e >= 2))]`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "5"
*/
RETURN [1, 5, 10][array::len([1, 2].filter(|$e| $e >= 2))];
