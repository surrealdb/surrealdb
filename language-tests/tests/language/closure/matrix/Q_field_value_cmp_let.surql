/**
[test]
reason = "The body is written in `DEFINE FIELD f ON w VALUE 10 >= $m`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: true, id: w:1 }]"

[[test.results]]
value = "[true]"
*/
LET $m = 8;
DEFINE FIELD f ON w VALUE 10 >= $m;
CREATE w:1;
SELECT VALUE f FROM w;
