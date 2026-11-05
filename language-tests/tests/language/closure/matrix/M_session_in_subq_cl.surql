/**
[test]
reason = "The body is written in `RETURN [1].map(|$e| (SELECT VALUE m FROM u WHERE $session.ns != NONE ORDER BY m LIMIT 1))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[{ id: u:1, m: 1 }]"

[[test.results]]
value = "[[1]]"
*/
CREATE u:1 SET m = 1;
RETURN [1].map(|$e| (SELECT VALUE m FROM u WHERE $session.ns != NONE ORDER BY m LIMIT 1));
