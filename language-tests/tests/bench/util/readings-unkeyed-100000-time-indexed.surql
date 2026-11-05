/**
# `readings-unkeyed-100000` plus an index on the stored `time` field: the
# baseline twin of `readings-keyed-100000-time-indexed`, with the same single
# index. `run = false`.

[env]
imports = ["./readings-unkeyed-100000.surql"]

[test]
run = false
*/

DEFINE INDEX by_time ON reading FIELDS time;
