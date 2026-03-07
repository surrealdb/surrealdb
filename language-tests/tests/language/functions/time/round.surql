/**
[test]
reason = "Test time::round() rounds a datetime to the nearest duration boundary"

[[test.results]]
value = "d'2024-06-15T13:00:00Z'"

[[test.results]]
value = "d'2024-06-15T12:00:00Z'"

*/
-- 34 minutes past the hour rounds up to next hour
time::round(d"2024-06-15T12:34:56Z", 1h);
-- 15 minutes past the hour rounds down
time::round(d"2024-06-15T12:15:00Z", 1h);
