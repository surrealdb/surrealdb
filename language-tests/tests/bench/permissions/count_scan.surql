/**
[env]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "COUNT over a permissioned table as a record user — FULL permissions count the key range without decoding a record, while a conditional permission has to judge each one and falls back to decoding the whole table. The `where_computed` arm is a control: the fallback evaluates the table permission against the decoded record and never builds field state, so COMPUTED fields on the table cost it nothing and it should stay level with `where` — a divergence means a count started materialising fields it only counts. The cost of a count that does need a derived value is `computed/count_filter`"
run = true
rebuild = false
datasets = { full = "bench/util/records-10000-perms-full.surql", where = "bench/util/records-10000-perms-where.surql", where_computed = "bench/util/records-10000-perms-where-computed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM record GROUP ALL;
