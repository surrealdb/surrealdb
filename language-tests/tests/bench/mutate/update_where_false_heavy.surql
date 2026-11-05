/**
[bench]
reason = "UPDATE of a table target under `WHERE false`: the subjects of the write are the empty scan, so no record is read and no stage runs, and the statement costs its compile. The statement binds every stage and removes those after the `WHERE` when the table's definitions let it: the `light` arm stores no event, view or `REFERENCE` field, so its stages after the `WHERE` are removed before they are lowered. The `heavy` arm stores events and views, whose units those stages request, so it lowers every stage and is the control for the removal. A repeated statement is served from the compile cache after the first run, so the median is the cached cost and the first-arrival cost is the difference."
run = true
rebuild = true
rebuild-file-backend = true
datasets = { heavy = "bench/util/records-10000-heavy-schema.surql", light = "bench/util/records-10000-light-schema.surql" }
warmup = "2s"
sample-size = 10
measurement-time = "20s"

[test]
run = false
*/

UPDATE record SET active = true WHERE false;
