/**
[test]
reason = "Test time::from_micros() converts microseconds to datetime"

[[test.results]]
value = "d'1970-01-01T00:00:00Z'"

[[test.results]]
value = "d'2024-06-15T12:30:00Z'"

*/
time::from_micros(0);
time::from_micros(1718454600000000);
