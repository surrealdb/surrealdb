/**
[test]
reason = "Test time::from_secs() and time::from_unix() convert seconds to datetime"

[[test.results]]
value = "d'1970-01-01T00:00:00Z'"

[[test.results]]
value = "d'2024-06-15T12:30:00Z'"

[[test.results]]
value = "d'2024-06-15T12:30:00Z'"

*/
time::from_secs(0);
time::from_secs(1718454600);
time::from_unix(1718454600);
