/**
[test]
reason = "Test time::floor() floors a datetime to a duration boundary"

[[test.results]]
value = "d'2024-06-15T12:00:00Z'"

[[test.results]]
value = "d'2024-06-15T00:00:00Z'"

[[test.results]]
value = "d'2024-06-15T12:30:00Z'"

*/
time::floor(d"2024-06-15T12:34:56Z", 1h);
time::floor(d"2024-06-15T12:34:56Z", 1d);
time::floor(d"2024-06-15T12:34:56Z", 30m);
