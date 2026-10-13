/**
[test]
reason = "The body is written in `DEFINE TABLE y AS SELECT v FROM x WHERE [v][WHERE array::any([$this], |$e| $e >= $m)] != []`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: x:1, v: 10 }]"

[[test.results]]
value = "[{ id: x:2, v: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10]"
*/
DEFINE PARAM $m VALUE 8;
CREATE x:1 SET v = 10;
CREATE x:2 SET v = 1;
DEFINE TABLE y AS SELECT v FROM x WHERE [v][WHERE array::any([$this], |$e| $e >= $m)] != [];
SELECT VALUE v FROM y;
