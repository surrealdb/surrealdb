/**
[test]
reason = "Test time::format() formats datetimes using format strings"

[[test.results]]
value = "'2024-06-15'"

[[test.results]]
value = "'12:30:00'"

[[test.results]]
value = "'2024-06-15 12:30:00'"

*/
time::format(d"2024-06-15T12:30:00Z", "%Y-%m-%d");
time::format(d"2024-06-15T12:30:00Z", "%H:%M:%S");
time::format(d"2024-06-15T12:30:00Z", "%Y-%m-%d %H:%M:%S");
