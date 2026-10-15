/**
[env]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "UPDATE every record of the table as a record user, returning nothing. Under RETURN NONE the select clause cannot change what the statement answers, so a select gate that is a stated FULL, NONE or absent clause is not run. The `full` arm states FULL and runs no select gate. The `where` and `where_derived` arms state a predicate over a field of the record, which the write cannot show to be free of raises and effects, so each record is judged against it once (`where_derived` derives `age_band` for the judgement first). In every arm the update predicate is judged before and after the change."
run = true
rebuild = true
rebuild-file-backend = true
datasets = { full = "bench/util/records-10000-perms-full.surql", where = "bench/util/records-10000-perms-where.surql", where_derived = "bench/util/records-10000-perms-where-derived.surql" }
warmup = "2s"
sample-size = 10
measurement-time = "20s"

[test]
run = false
*/

UPDATE record SET active = true RETURN NONE;
