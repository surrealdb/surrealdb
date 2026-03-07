/**
[test]
reason = "Test time::yday() returns the day of the year"

[[test.results]]
value = "1"

[[test.results]]
value = "167"

[[test.results]]
value = "366"

*/
time::yday(d"2024-01-01T00:00:00Z");
time::yday(d"2024-06-15T12:00:00Z");
-- 2024 is a leap year, so Dec 31 is day 366
time::yday(d"2024-12-31T23:59:59Z");
