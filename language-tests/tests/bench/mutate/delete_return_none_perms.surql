/**
[env]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "DELETE the records matching a predicate as a record user, returning nothing. Under RETURN NONE the select clause cannot change what the statement answers, so a select gate that is a stated FULL, NONE or absent clause is not run. The `full` arm states FULL and runs no select gate. The `where` and `where_derived` arms state a predicate over a field of the record, which the write cannot show to be free of raises and effects, so the pre-image of each removed record is judged against it (`where_derived` derives `age_band` for the judgement first)."
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

DELETE record WHERE age <= 50 RETURN NONE;
