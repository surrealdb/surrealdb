/**
[test]
reason = "Test time::ceil() ceils a datetime to the next duration boundary"

[[test.results]]
value = "d'2024-06-15T13:00:00Z'"

[[test.results]]
value = "d'2024-06-16T00:00:00Z'"

[[test.results]]
value = "d'2024-06-15T13:00:00Z'"

*/
time::ceil(d"2024-06-15T12:34:56Z", 1h);
time::ceil(d"2024-06-15T12:34:56Z", 1d);
time::ceil(d"2024-06-15T12:34:56Z", 30m);
