/**
[test]
reason = "Test time::day() extracts the day of the month"

[[test.results]]
value = "1"

[[test.results]]
value = "15"

[[test.results]]
value = "28"

*/
time::day(d"2024-01-01T00:00:00Z");
time::day(d"2024-06-15T12:30:00Z");
time::day(d"2024-02-28T23:59:59Z");
