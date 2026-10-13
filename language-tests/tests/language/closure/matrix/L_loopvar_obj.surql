/**
[test]
reason = "The body is written in `FOR $m IN [8] { CREATE out SET v = { k: $m }; }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[8]"
*/
FOR $m IN [8] { CREATE out SET v = { k: $m }; };
SELECT VALUE v.k FROM out;
