/**
[test]
reason = "The body is written in `FOR $m IN [[0, 8, 100]] { CREATE out SET v = $m[1]; }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[8]"
*/
FOR $m IN [[0, 8, 100]] { CREATE out SET v = $m[1]; };
SELECT VALUE v FROM out;
