/**
[test]
reason = "Test time::micros converting a datetime to microseconds since the Unix epoch"

[[test.results]]
value = "0"

[[test.results]]
value = "1704067200000000"

[[test.results]]
value = "1"

[[test.results]]
value = "-1000000"

[[test.results]]
value = "-1"

[[test.results]]
error = "Incorrect arguments for function time::micros(). Argument 1 was the wrong type. Expected `datetime` but found `'not a datetime'`"
*/

time::micros(d"1970-01-01T00:00:00Z");
time::micros(d"2024-01-01T00:00:00Z");
time::micros(d"1970-01-01T00:00:00.000001Z");
time::micros(d"1969-12-31T23:59:59Z");
time::micros(d"1969-12-31T23:59:59.999999Z");
time::micros("not a datetime");
