/**
# `readings-unkeyed-100000` plus a compound index on (city, time): how an
# unkeyed table answers a per-city time window or the latest rows of a city,
# against the record-key range of `readings-keyed-100000`. `run = false`.

[env]
imports = ["./readings-unkeyed-100000.surql"]

[test]
run = false
*/

DEFINE INDEX by_city_time ON reading FIELDS city, time;
