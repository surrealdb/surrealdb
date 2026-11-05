/**
[env]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "A table carrying both COMPUTED fields and a field-level SELECT permission: no guard at all, a guard satisfied by every row (per-record unreduced-record clone, nothing withheld), and a guard that withholds a field for half of them (every computed field the read needs derived a second time, against the reduced record). The projection names the three computed fields that read nothing restricted, so all three arms return identical documents while the guard on `status` is enforced regardless — field permissions are never narrowed to the projection — and what the withholding arm re-derives is exactly the work that did not need redoing"
run = true
rebuild = false
datasets = { none = "bench/util/records-10000-computed-perms.surql", open = "bench/util/records-10000-computed-guarded-open.surql", cut = "bench/util/records-10000-computed-guarded-cut.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT label, bucket, word_count FROM record;
