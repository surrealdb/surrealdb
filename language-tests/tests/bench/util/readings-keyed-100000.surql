/**
# 100k `reading` rows over 50 cities, in a table whose record id names its
# components (`[city: string, time: datetime]`), for the time-series key-range
# benches.
#
# Row `$i` belongs to city `city-($i % 50)`, so each city holds 2000 rows, and
# was read at 2024-01-01T00:00:00Z plus `$i * 863` ms. The step shares no factor
# with 1000, so the sub-second part takes every value over a thousand rows, a
# whole-second bound lands next to rows that share its second, and each city is
# read about every 43 s across one day (a one-hour window holds about 83 of its
# rows). No two rows share a time, so no two share a key. `temp` and `humidity`
# are functions of `$i` and nothing is drawn from the RNG, so the unkeyed twin
# (`readings-unkeyed-100000`) holds the same documents row for row.
#
# `run = false`.

[env]
imports = ["./readings-keyed-empty.surql"]

[test]
run = false
*/

FOR $i IN 0..100000 {
	CREATE reading SET
		city = string::concat('city-', $i % 50),
		time = time::from_millis(1704067200000 + $i * 863),
		temp = (($i * 37) % 500) / 10.0 - 10.0,
		humidity = 20 + ($i * 13) % 80
		RETURN NONE;
};
