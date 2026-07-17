/**
[test]
reason = "Test time::millis converting a datetime to milliseconds since the Unix epoch"

[[test.results]]
value = "0"

[[test.results]]
value = "1704067200000"

[[test.results]]
value = "1"

[[test.results]]
value = "-1000"

[[test.results]]
value = "-1"

[[test.results]]
error = "Incorrect arguments for function time::millis(). Argument 1 was the wrong type. Expected `datetime` but found `'not a datetime'`"
*/

time::millis(d"1970-01-01T00:00:00Z");
time::millis(d"2024-01-01T00:00:00Z");
time::millis(d"1970-01-01T00:00:00.001Z");
time::millis(d"1969-12-31T23:59:59Z");
time::millis(d"1969-12-31T23:59:59.999Z");
time::millis("not a datetime");
