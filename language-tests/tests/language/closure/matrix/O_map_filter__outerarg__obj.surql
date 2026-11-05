/**
[test]
reason = "The body is written in `RETURN array::map([[1, 5], [10, 15, 20]], |$o| array::filter($o, |$i| $i >= { k: $o }.k))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[[], []]"
*/
RETURN array::map([[1, 5], [10, 15, 20]], |$o| array::filter($o, |$i| $i >= { k: $o }.k));
