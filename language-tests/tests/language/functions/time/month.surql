/**
[test]
reason = "Test time::month() extracts the month component"

[[test.results]]
value = "1"

[[test.results]]
value = "6"

[[test.results]]
value = "12"

*/
time::month(d"2024-01-01T00:00:00Z");
time::month(d"2024-06-15T12:30:00Z");
time::month(d"2024-12-31T23:59:59Z");
