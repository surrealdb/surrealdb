/**
[test]
reason = "Test time::micros() returns microseconds since Unix epoch"

[[test.results]]
value = "0"

[[test.results]]
value = "1718454600000000"

*/
time::micros(d"1970-01-01T00:00:00Z");
time::micros(d"2024-06-15T12:30:00Z");
