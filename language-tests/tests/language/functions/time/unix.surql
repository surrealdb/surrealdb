/**
[test]
reason = "Test time::unix converting a datetime to seconds since the Unix epoch"

[[test.results]]
value = "0"

[[test.results]]
value = "1704067200"

[[test.results]]
value = "0"

[[test.results]]
value = "-1"

[[test.results]]
value = "-1"

[[test.results]]
error = "Incorrect arguments for function time::unix(). Argument 1 was the wrong type. Expected `datetime` but found `'not a datetime'`"
*/

time::unix(d"1970-01-01T00:00:00Z");
time::unix(d"2024-01-01T00:00:00Z");
time::unix(d"1970-01-01T00:00:00.999Z");
time::unix(d"1969-12-31T23:59:59Z");
time::unix(d"1969-12-31T23:59:59.500Z");
time::unix("not a datetime");
