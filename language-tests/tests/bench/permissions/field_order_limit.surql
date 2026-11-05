/**
[env]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "ORDER BY … LIMIT whose sort field carries a field-level SELECT permission — the TopK threshold probe reads the sort key straight from revision-encoded bytes, so a restricted field leaves it dormant and every row pays a full decode instead of being rejected against the heap's worst entry"
run = true
rebuild = false
datasets = { open = "bench/util/records-10000-perms-full.surql", guarded = "bench/util/records-10000-fieldperms-one.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record ORDER BY number DESC LIMIT 20;
