/**
[bench]
reason = "Full-table SELECT * over 100k readings, against the same values stored in the body. In the keyed arm city and time are not stored: every row reads them from its record id through a read-time layer. In the stored arm they are ordinary fields of a record with a random id. Both arms return documents with the same fields and values apart from the id, so the delta is the per-row cost of deriving the two components rather than a different result."
run = true
rebuild = false
datasets = { stored = "bench/util/readings-unkeyed-100000.surql", keyed = "bench/util/readings-keyed-100000.surql" }
warmup = "2s"
sample-size = 20
measurement-time = "30s"

[test]
run = false
*/

SELECT * FROM reading;
