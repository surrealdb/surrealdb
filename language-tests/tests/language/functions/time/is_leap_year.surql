/**
[test]
reason = "Test time::is_leap_year() checks if the year is a leap year"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

*/
time::is_leap_year(d"2024-01-01T00:00:00Z");
time::is_leap_year(d"2023-01-01T00:00:00Z");
time::is_leap_year(d"2000-01-01T00:00:00Z");
