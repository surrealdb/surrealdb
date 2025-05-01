/**
[test]

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "d'1970-01-01T00:00:00Z'"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

*/
type::is::datetime(rand::time());
type::is::datetime(rand::time(1577836800, 1893456000));
rand::time(d'1970-01-01', d'1970-01-01');
rand::time(d'1970-01-01', time::now()).is_datetime();
rand::time(0, 100).is_datetime();
rand::time(100, 0).is_datetime();
rand::time(0, d'2025-01-01').is_datetime();
rand::time(d'2025-01-01', 0).is_datetime();
rand::time(d'2020-01-01', d'2025-01-01').is_datetime();