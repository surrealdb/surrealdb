/**
[test]
reason = "Test time::nano converting a datetime to nanoseconds since the Unix epoch"

[[test.results]]
value = "0"

[[test.results]]
value = "1704067200000000000"

[[test.results]]
value = "1"

[[test.results]]
value = "-1000000000"

[[test.results]]
value = "9223372036854775807"

[[test.results]]
error = "Failed to compute: \"time::nano(2262-04-11T23:47:16.854775808Z)\", as the operation results in an arithmetic overflow."

[[test.results]]
error = "Failed to compute: \"time::nano(2500-01-01T00:00:00Z)\", as the operation results in an arithmetic overflow."

[[test.results]]
error = "Incorrect arguments for function time::nano(). Argument 1 was the wrong type. Expected `datetime` but found `'not a datetime'`"
*/

time::nano(d"1970-01-01T00:00:00Z");
time::nano(d"2024-01-01T00:00:00Z");
time::nano(d"1970-01-01T00:00:00.000000001Z");
time::nano(d"1969-12-31T23:59:59Z");
time::nano(d"2262-04-11T23:47:16.854775807Z");
time::nano(d"2262-04-11T23:47:16.854775808Z");
time::nano(d"2500-01-01T00:00:00Z");
time::nano("not a datetime");
