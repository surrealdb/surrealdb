/**
[test]
reason = "The body is written in `FOR $o IN [{ a: [1, 5, 10] }] { CREATE out SET v = $o.a.filter(|$e| $e >= 5); }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[5, 10]]"
*/
FOR $o IN [{ a: [1, 5, 10] }] { CREATE out SET v = $o.a.filter(|$e| $e >= 5); };
SELECT VALUE v FROM out;
