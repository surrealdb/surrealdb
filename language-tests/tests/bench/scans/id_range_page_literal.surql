/**
[env]
imports = ["bench/util/records-seq-100000.surql"]

[bench]
reason = "Page a table by a literal record-id bound, as a client pages by the last id it read"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "20s"

[test]
run = false
*/

SELECT id, name FROM record WHERE id > record:60000 ORDER BY id LIMIT 100;
