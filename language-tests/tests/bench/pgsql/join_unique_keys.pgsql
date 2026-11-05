/**
[env]
imports = ["bench/util/records-100000.surql"]

[bench]
reason = "PostgreSQL inner join of the 100k-row table with itself on a unique key: one hash table of 100k build rows, 100k probe rows, 100k joined rows out"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "15s"

[test]
run = false
*/

SELECT a.name, b.city FROM record a JOIN record b ON a.integer = b.integer;
