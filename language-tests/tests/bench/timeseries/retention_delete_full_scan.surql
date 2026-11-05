/**
[bench]
reason = "Retention over 100k readings with no index: delete the oldest tenth, the 10012 rows before 02:24 of a one-day series, with the cutoff bound in a LET. A bound on time alone cannot be a key range while the city is unpinned, so every arm scans the whole table and judge every row. On the keyed table time is the derived component of the record id, read from the key of each scanned row. The array_ids arm has the same keys, an id that names nothing and city and time stored in each body, so keyed against array_ids is the cost of deriving the components in place of reading stored fields; the unkeyed arm stores the same fields under random ids, so keyed against unkeyed adds what the longer array keys cost to read and remove. The delete returns nothing, so the row count is not a check on it. Every iteration re-imports 100k rows before the delete is timed, so a sample costs seconds: few samples on purpose."
run = true
rebuild = true
datasets = { keyed = "bench/util/readings-keyed-100000.surql", array_ids = "bench/util/readings-array-ids-100000.surql", unkeyed = "bench/util/readings-unkeyed-100000.surql" }
warmup = "2s"
sample-size = 10
measurement-time = "90s"
max-time = "300s"

[test]
run = false
*/

LET $cut = d'2024-01-02T00:00:00Z' - 21h36m;
DELETE reading WHERE time < $cut;
