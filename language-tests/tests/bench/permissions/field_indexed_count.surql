/**
[env]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "Indexed COUNT whose WHERE field carries a field-level SELECT permission — counting index entries never reduces a document, so the planner declines the indexed-count fast paths and falls back to a scan that materialises and reduces every matching row"
run = true
rebuild = false
datasets = { open = "bench/util/records-10000-indexed-perms-full.surql", guarded = "bench/util/records-10000-indexed-fieldperms-one.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM record WHERE number > 4000 GROUP ALL;
