/**
[bench]
reason = "Upsert 120 records by evaluated id, one statement per FOR iteration, in one transaction"
run = true
rebuild = true
warmup = "2s"
sample-size = 30
measurement-time = "20s"

[test]
run = false
*/

BEGIN TRANSACTION;
FOR $i IN 0..120 {
	UPSERT type::record('record', ['s', $i]) CONTENT { a: 1 };
};
COMMIT TRANSACTION;
