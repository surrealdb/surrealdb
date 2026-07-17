/**
[test]
reason = "Test time::ceil rounding datetimes up to a duration boundary"

[[test.results]]
value = "d'2024-05-05T13:00:00Z'"

[[test.results]]
value = "d'2024-05-05T12:00:00Z'"

[[test.results]]
value = "d'2024-05-06T00:00:00Z'"

[[test.results]]
value = "d'2024-05-05T12:34:57Z'"

[[test.results]]
value = "d'2024-05-09T00:00:00Z'"

[[test.results]]
value = "d'2024-05-05T12:34:56Z'"

[[test.results]]
error = "Incorrect arguments for function time::ceil(). The second argument must be a duration, and must be able to be represented as nanoseconds."

[[test.results]]
error = "Incorrect arguments for function time::ceil(). Argument 2 was the wrong type. Expected `duration` but found `'1h'`"

[[test.results]]
error = "Incorrect arguments for function time::ceil(). Argument 1 was the wrong type. Expected `datetime` but found `NONE`"
*/

time::ceil(d"2024-05-05T12:34:56Z", 1h);
time::ceil(d"2024-05-05T12:00:00Z", 1h);
time::ceil(d"2024-05-05T12:34:56Z", 1d);
time::ceil(d"2024-05-05T12:34:56.123456789Z", 1s);
time::ceil(d"2024-05-05T12:34:56Z", 1w);
time::ceil(d"2024-05-05T12:34:56Z", 0ns);
time::ceil(d"2024-05-05T12:34:56Z", 10000000000w);
time::ceil(d"2024-05-05T12:34:56Z", "1h");
time::ceil(NONE, 1h);
