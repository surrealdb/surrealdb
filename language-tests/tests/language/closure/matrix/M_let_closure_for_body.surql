/**
[test]
reason = "The body is written in `RETURN array::map([[1, 10]], |$r| { LET $c = 0; FOR $x IN $r { IF $x >= $m { LET $c = $c + 1; } }; $c })`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[0]"
*/
LET $m = 8;
RETURN array::map([[1, 10]], |$r| { LET $c = 0; FOR $x IN $r { IF $x >= $m { LET $c = $c + 1; } }; $c });
