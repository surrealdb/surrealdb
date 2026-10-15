/**
[bench]
reason = "UPDATE every record of a table whose four events are never fired by an UPDATE, returning nothing. The `dormant` arm holds the events and the `none` arm holds none; a write that decides ahead of time which events can fire pays for the events in neither arm by more than their depth check, so the two are expected to match."
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

UPDATE record SET active = true RETURN NONE;
