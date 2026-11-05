/**
[env]
imports = ["bench/util/records-10000-computed-perms.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "`update_10000_return_after_computed` as a record user: the post-image is shown through the table's field view, cut to the fields the caller may select, so a pipeline runs over a copy of every image. This guards the path that keeps its copy"
run = true
rebuild = true
rebuild-file-backend = true
warmup = "2s"
sample-size = 10
measurement-time = "20s"

[test]
run = false
*/

UPDATE record SET active = true RETURN AFTER;
