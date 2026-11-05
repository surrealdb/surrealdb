/**
[bench]
reason = "Create 1000 records one statement per FOR iteration, each in a table the statement names"
run = true
rebuild = true
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

FOR $i IN 0..1000 {
	CREATE record SET n = $i, name = <string> $i, active = $i % 2 = 0
};
