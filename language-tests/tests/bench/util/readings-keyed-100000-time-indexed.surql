/**
# `readings-keyed-100000` plus an index on `time`, a component of the record id:
# the index a retention delete reads, since a bound on `time` alone cannot be a
# key range while the city is unpinned. The index column is the derived
# component, not a stored field. `run = false`.

[env]
imports = ["./readings-keyed-100000.surql"]

[test]
run = false
*/

DEFINE INDEX by_time ON reading FIELDS time;
