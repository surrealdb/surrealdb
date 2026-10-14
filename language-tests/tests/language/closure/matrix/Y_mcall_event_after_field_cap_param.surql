/**
[test]
reason = "The body is written in `DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = $after.a.filter(|$e| $e >= $m) }`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: [1, 5, 10], id: src:1 }]"

[[test.results]]
value = "[[5, 10]]"
*/
DEFINE PARAM $m VALUE 5;
DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = $after.a.filter(|$e| $e >= $m) };
CREATE src:1 SET a = [1, 5, 10];
SELECT VALUE v FROM log;
