/**
[env]
imports = ["bench/util/records-seq-100000.surql"]

[bench]
reason = "Page a table by a record-id bound held in a parameter"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "20s"

[test]
run = false
*/

LET $c = record:60000;
SELECT id, name FROM record WHERE id > $c ORDER BY id LIMIT 100;
