/**
[env]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "Full-table SELECT as a record user with field-level SELECT permissions — measures the per-record field-guard walk (no guards vs. one guarded field vs. eight), all guards satisfied so every arm returns the same documents"
run = true
rebuild = false
datasets = { none = "bench/util/records-10000-perms-full.surql", one = "bench/util/records-10000-fieldperms-one.surql", many = "bench/util/records-10000-fieldperms-many.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record;
