/**
[bench]
reason = "Retention over 100k readings: delete the oldest tenth, the 10012 rows before 02:24 of a one-day series, with the cutoff bound in a LET. The only index is on time: on the keyed table it is the index over the derived time component of the record id, and on the unkeyed table over the stored field. A bound on time alone cannot be a key range while the city is unpinned, so both arms read the ids through that index and delete each row with its index entry. The LET stands for the form `LET $cut = time::now() - <retention>`: the clock is read once into the parameter, because a clock read written in the WHERE of a mutation is evaluated per row and bounds no index range. The delete returns nothing, so the row count is not a check on it. Every iteration re-imports 100k rows and builds the index before the delete is timed, so a sample costs seconds: few samples on purpose."
run = true
rebuild = true
datasets = { keyed = "bench/util/readings-keyed-100000-time-indexed.surql", unkeyed = "bench/util/readings-unkeyed-100000-time-indexed.surql" }
warmup = "2s"
sample-size = 10
measurement-time = "90s"
max-time = "300s"

[test]
run = false
*/

LET $cut = d'2024-01-02T00:00:00Z' - 21h36m;
DELETE reading WHERE time < $cut;
