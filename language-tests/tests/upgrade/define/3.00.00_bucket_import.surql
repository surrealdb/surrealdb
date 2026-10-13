/**
[env.capabilities]
allow-experimental = ["files"]

[test]
reason = "Setup for bucket.surql: the old binary defines the buckets whose stored definitions the new one has to read back."
run = false
*/
DEFINE BUCKET bkt BACKEND "memory";
DEFINE BUCKET bkt_ro BACKEND "memory" READONLY;
DEFINE BUCKET bkt_perm BACKEND "memory" PERMISSIONS WHERE $action = "get";
