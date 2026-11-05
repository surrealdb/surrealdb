/**
# An empty `reading` table with the ordinary random record id: the baseline
# twin of `readings-keyed-empty`. A record stores `city` and `time` in its
# body like any other field.
#
# The setup of `readings-unkeyed-100000` and of the create-throughput bench.
# `run = false`.

[test]
run = false
*/

DEFINE TABLE reading SCHEMALESS;
