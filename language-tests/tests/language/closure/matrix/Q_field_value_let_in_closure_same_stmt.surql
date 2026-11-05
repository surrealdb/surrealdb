/**
[test]
reason = "The body is written in `DEFINE FIELD f ON w VALUE [1, 10].map(|$e| $e + $value)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: [6, 15], id: w:1 }]"

[[test.results]]
value = "[[6, 15]]"
*/
DEFINE FIELD f ON w VALUE [1, 10].map(|$e| $e + $value);
CREATE w:1 SET f = 5;
SELECT VALUE f FROM w;
