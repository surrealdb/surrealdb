/**
# `readings-unkeyed-empty` plus the compound (city, time) index an unkeyed
# time-series table uses for a per-city time window: the index the key of
# `readings-keyed-empty` stands in for. `run = false`.

[env]
imports = ["./readings-unkeyed-empty.surql"]

[test]
run = false
*/

DEFINE INDEX by_city_time ON reading FIELDS city, time;
