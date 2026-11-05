/**
[test]
reason = "The body is written in `DEFINE FIELD f ON w VALUE $m`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 8, id: w:1 }]"

[[test.results]]
value = "[8]"
*/
DEFINE PARAM $m VALUE 8;
DEFINE FIELD f ON w VALUE $m;
CREATE w:1;
SELECT VALUE f FROM w;
