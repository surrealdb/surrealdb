/**
# The baseline twin of `readings-keyed-100000`: the same 100k rows, with the
# same `city`, `time`, `temp` and `humidity` values, in a table whose records
# have random ids and store `city` and `time` in the body. No index, so a read
# of a city or a time range scans the table. `run = false`.

[env]
imports = ["./readings-unkeyed-empty.surql"]

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
