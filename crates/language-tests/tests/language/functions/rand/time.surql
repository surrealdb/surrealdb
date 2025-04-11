/**
[test]

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
d'1970-01-01T00:00:00Z'

[[test.results]]
true

*/
type::is::datetime(rand::time());
type::is::datetime(rand::time(1577836800, 1893456000));
rand::time(d'1970-01-01', d'1970-01-01');
rand::time(d'1970-01-01', time::now()).is_datetime();