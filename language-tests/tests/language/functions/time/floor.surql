/**
[test]
reason = "Test time::floor rounding datetimes down to a duration boundary"

[[test.results]]
value = "d'2024-05-05T12:00:00Z'"

[[test.results]]
value = "d'2024-05-05T12:00:00Z'"

[[test.results]]
value = "d'2024-05-05T00:00:00Z'"

[[test.results]]
value = "d'2024-05-05T12:34:56Z'"

[[test.results]]
value = "d'2024-05-02T00:00:00Z'"

[[test.results]]
value = "d'2024-05-05T12:34:56Z'"

[[test.results]]
error = "Incorrect arguments for function time::floor(). The second argument must be a duration, and must be able to be represented as nanoseconds."

[[test.results]]
error = "Incorrect arguments for function time::floor(). Argument 2 was the wrong type. Expected `duration` but found `'1h'`"
*/

time::floor(d"2024-05-05T12:34:56Z", 1h);
time::floor(d"2024-05-05T12:00:00Z", 1h);
time::floor(d"2024-05-05T12:34:56Z", 1d);
time::floor(d"2024-05-05T12:34:56.123456789Z", 1s);
time::floor(d"2024-05-05T12:34:56Z", 1w);
time::floor(d"2024-05-05T12:34:56Z", 0ns);
time::floor(d"2024-05-05T12:34:56Z", 10000000000w);
time::floor(d"2024-05-05T12:34:56Z", "1h");
