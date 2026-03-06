/**
[test]
reason = "Test time::unix() returns seconds since Unix epoch"

[[test.results]]
value = "0"

[[test.results]]
value = "1718454600"

*/
time::unix(d"1970-01-01T00:00:00Z");
time::unix(d"2024-06-15T12:30:00Z");
