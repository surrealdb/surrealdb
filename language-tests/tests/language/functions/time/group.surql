/**
[test]
reason = "Test time::group() groups a datetime by a time component"

[[test.results]]
value = "d'2024-01-01T00:00:00Z'"

[[test.results]]
value = "d'2024-06-01T00:00:00Z'"

[[test.results]]
value = "d'2024-06-15T00:00:00Z'"

[[test.results]]
value = "d'2024-06-15T12:00:00Z'"

[[test.results]]
value = "d'2024-06-15T12:34:00Z'"

[[test.results]]
value = "d'2024-06-15T12:34:56Z'"

*/
time::group(d"2024-06-15T12:34:56Z", "year");
time::group(d"2024-06-15T12:34:56Z", "month");
time::group(d"2024-06-15T12:34:56Z", "day");
time::group(d"2024-06-15T12:34:56Z", "hour");
time::group(d"2024-06-15T12:34:56Z", "minute");
time::group(d"2024-06-15T12:34:56Z", "second");
