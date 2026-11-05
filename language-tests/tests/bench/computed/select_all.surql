/**
[bench]
reason = "Full-table SELECT * over a table with three read-time COMPUTED fields, against the same field names and values written into the records as ordinary stored data — both arms return identical documents, so the delta is per-record materialisation and the field state it needs rather than a wider result object"
run = true
rebuild = false
datasets = { stored = "bench/util/records-10000-computed-stored.surql", computed = "bench/util/records-10000-computed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record;
