/**
# 100k `reading` rows under array record ids `[city, time]` whose id is NOT
# named: the table a `DEFINE FIELD id ON reading TYPE [city: string, time:
# datetime]` converts. The rows have the cities and times of
# `readings-keyed-100000`, and each body also stores `city` and `time`, equal to
# the matching element of its key, so a conversion has bodies to compare as well
# as keys to check. `run = false`.

[test]
run = false
*/

DEFINE TABLE reading SCHEMALESS;
FOR $i IN 0..100000 {
	LET $city = string::concat('city-', $i % 50);
	LET $time = time::from_millis(1704067200000 + $i * 863);
	CREATE type::record('reading', [$city, $time]) SET
		city = $city,
		time = $time,
		temp = (($i * 37) % 500) / 10.0 - 10.0,
		humidity = 20 + ($i * 13) % 80
		RETURN NONE;
};
