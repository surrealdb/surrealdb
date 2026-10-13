/**
[test]
reason = "Test time::week extracting the ISO week number from a datetime"

[[test.results]]
value = "1"

[[test.results]]
value = "53"

[[test.results]]
value = "1"

[[test.results]]
value = "53"

[[test.results]]
value = "27"

[[test.results]]
error = "Incorrect arguments for function time::week(). Argument 1 was the wrong type. Expected `datetime` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function time::week(). Argument 1 was the wrong type. Expected `datetime` but found `'2024-01-04T00:00:00Z'`"
*/

time::week(d"2024-01-04T00:00:00Z");
time::week(d"2021-01-01T00:00:00Z");
time::week(d"2024-12-30T00:00:00Z");
time::week(d"2020-12-31T00:00:00Z");
time::week(d"2024-07-01T00:00:00Z");
time::week(NONE);
time::week("2024-01-04T00:00:00Z");
