/**
[test]
reason = """
Setup for 3.00.00_field_temporal_retype_interval.surql: the old binary writes
`duration` and `option<duration>` fields holding whole units, microsecond
precision, a value finer than a microsecond and zero, so the new binary can cast
them to `interval`."""
run = false
*/
DEFINE TABLE ri SCHEMAFULL;
DEFINE FIELD d ON ri TYPE duration;
DEFINE FIELD o ON ri TYPE option<duration>;
CREATE ri:1 SET d = 1h30m, o = 2d;
CREATE ri:2 SET d = 1s250ms;
CREATE ri:3 SET d = 5us, o = 0s;
CREATE ri:4 SET d = 1ns;
