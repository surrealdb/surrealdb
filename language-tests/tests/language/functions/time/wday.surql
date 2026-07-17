/**
[test]
reason = "Test time::wday extracting the ISO weekday number (Monday = 1) from a datetime"

[[test.results]]
value = "1"

[[test.results]]
value = "7"

[[test.results]]
value = "3"

[[test.results]]
value = "7"

[[test.results]]
error = "Incorrect arguments for function time::wday(). Argument 1 was the wrong type. Expected `datetime` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function time::wday(). Argument 1 was the wrong type. Expected `datetime` but found `'2024-01-01T00:00:00Z'`"
*/

time::wday(d"2024-01-01T00:00:00Z");
time::wday(d"2024-01-07T00:00:00Z");
time::wday(d"2024-05-08T12:34:56Z");
time::wday(d"2024-01-01T00:30:00+01:00");
time::wday(NONE);
time::wday("2024-01-01T00:00:00Z");
