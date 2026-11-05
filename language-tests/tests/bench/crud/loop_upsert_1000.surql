/**
[bench]
reason = "Upsert 1000 records by evaluated id, one statement per FOR iteration"
run = true
rebuild = true
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

FOR $i IN 0..1000 {
	UPSERT type::record('record', $i) SET n = $i, name = <string> $i
};
