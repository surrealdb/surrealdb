/**
[env]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "Filter scan whose WHERE field carries a field-level SELECT permission — a restricted field cannot be read from revision-encoded bytes, so the pre-decode reject filter is withheld and every row is fully decoded before the predicate runs"
run = true
rebuild = false
datasets = { open = "bench/util/records-10000-perms-full.surql", guarded = "bench/util/records-10000-fieldperms-one.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record WHERE number > 4000;
