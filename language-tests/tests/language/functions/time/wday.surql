/**
[test]
reason = "Test time::wday() returns the weekday (1=Monday to 7=Sunday)"

[[test.results]]
value = "1"

[[test.results]]
value = "6"

[[test.results]]
value = "7"

*/
-- 2024-01-01 is a Monday
time::wday(d"2024-01-01T00:00:00Z");
-- 2024-06-15 is a Saturday
time::wday(d"2024-06-15T12:00:00Z");
-- 2024-06-16 is a Sunday
time::wday(d"2024-06-16T12:00:00Z");
