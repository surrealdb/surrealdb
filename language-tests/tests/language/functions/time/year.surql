/**
[test]
reason = "Test time::year() extracts the year component"

[[test.results]]
value = "2024"

[[test.results]]
value = "1970"

[[test.results]]
value = "2000"

*/
time::year(d"2024-06-15T12:30:00Z");
time::year(d"1970-01-01T00:00:00Z");
time::year(d"2000-12-31T23:59:59Z");
