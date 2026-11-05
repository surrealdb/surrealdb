/**
[bench]
reason = "DELETE every record of a table with four events, two of which a DELETE fires. The `dormant` arm holds the events and the `none` arm holds none; the events that name another action cost a DELETE no more than their depth check, so the `dormant` arm pays for the two events that fire and no more."
run = true
rebuild = true
rebuild-file-backend = true
datasets = { dormant = "bench/util/records-10000-events-dormant.surql", none = "bench/util/records-10000-events-none.surql" }
warmup = "2s"
sample-size = 10
measurement-time = "20s"

[test]
run = false
*/

DELETE record WHERE number > 0 RETURN NONE;
