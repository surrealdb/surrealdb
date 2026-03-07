/**
[test]
reason = "Test time::set_* functions for modifying datetime components"

[[test.results]]
value = "d'2025-06-15T12:30:00Z'"

[[test.results]]
value = "d'2024-03-15T12:30:00Z'"

[[test.results]]
value = "d'2024-06-20T12:30:00Z'"

[[test.results]]
value = "d'2024-06-15T08:30:00Z'"

[[test.results]]
value = "d'2024-06-15T12:45:00Z'"

[[test.results]]
value = "d'2024-06-15T12:30:30Z'"

*/
time::set_year(d"2024-06-15T12:30:00Z", 2025);
time::set_month(d"2024-06-15T12:30:00Z", 3);
time::set_day(d"2024-06-15T12:30:00Z", 20);
time::set_hour(d"2024-06-15T12:30:00Z", 8);
time::set_minute(d"2024-06-15T12:30:00Z", 45);
time::set_second(d"2024-06-15T12:30:00Z", 30);
