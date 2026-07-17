/**
[test]
reason = "Test time::yday extracting the day of the year from a datetime"

[[test.results]]
value = "1"

[[test.results]]
value = "366"

[[test.results]]
value = "365"

[[test.results]]
value = "61"

[[test.results]]
value = "60"

[[test.results]]
error = "Incorrect arguments for function time::yday(). Argument 1 was the wrong type. Expected `datetime` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function time::yday(). Argument 1 was the wrong type. Expected `datetime` but found `'2024-01-01T00:00:00Z'`"
*/

time::yday(d"2024-01-01T00:00:00Z");
time::yday(d"2024-12-31T00:00:00Z");
time::yday(d"2023-12-31T00:00:00Z");
time::yday(d"2024-03-01T00:00:00Z");
time::yday(d"2023-03-01T00:00:00Z");
time::yday(NONE);
time::yday("2024-01-01T00:00:00Z");
