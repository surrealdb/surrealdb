/**
[bench]
reason = "The latest rows of a schemaless table by an indexed column with no declared kind: an index that has stored no array in the column delivers its order, so the scan reads ten entries from its end, where a column that may hold an array has every row read and sorted."
run = true
rebuild = false
datasets = { indexed = "bench/util/records-10000-rule-planner.surql", typed = "bench/util/records-10000-rule-planner-typed.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT id, created_at FROM record ORDER BY created_at DESC LIMIT 10;
