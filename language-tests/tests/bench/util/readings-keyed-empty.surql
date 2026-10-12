/**
# An empty `reading` table whose record id names its components, a city and a
# datetime: `city` is `id[0]` and `time` is `id[1]` on every row, derived from
# the key and never stored in the body. A record is created by writing the
# components (`CREATE reading SET city = ..., time = ...`), which build its key.
#
# The setup of `readings-keyed-100000` and of the create-throughput bench.
# `run = false`.

[test]
run = false
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
