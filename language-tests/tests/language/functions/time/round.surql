/**
[test]
reason = "Test time::round rounding datetimes to the nearest duration boundary"

[[test.results]]
value = "d'2024-05-05T12:00:00Z'"

[[test.results]]
value = "d'2024-05-05T13:00:00Z'"

[[test.results]]
value = "d'2024-05-06T00:00:00Z'"

[[test.results]]
value = "d'2024-05-05T00:00:00Z'"

[[test.results]]
value = "d'2024-05-06T00:00:00Z'"

[[test.results]]
value = "d'2024-05-05T12:34:56Z'"

[[test.results]]
value = "d'2024-05-05T12:34:56Z'"

[[test.results]]
error = "Incorrect arguments for function time::round(). The second argument must be a duration, and must be able to be represented as nanoseconds."

[[test.results]]
error = "Incorrect arguments for function time::round(). Argument 2 was the wrong type. Expected `duration` but found `'1h'`"
*/

time::round(d"2024-05-05T12:29:59Z", 1h);
time::round(d"2024-05-05T12:30:00Z", 1h);
time::round(d"2024-05-05T12:34:56Z", 1d);
time::round(d"2024-05-05T11:59:59Z", 1d);
time::round(d"2024-05-05T12:00:00Z", 1d);
time::round(d"2024-05-05T12:34:56.499999999Z", 1s);
time::round(d"2024-05-05T12:34:56Z", 0ns);
time::round(d"2024-05-05T12:34:56Z", 10000000000w);
time::round(d"2024-05-05T12:34:56Z", "1h");
