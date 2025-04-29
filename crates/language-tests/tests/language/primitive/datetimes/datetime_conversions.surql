/**
[test]

[[test.results]]
value = "['2012-01-01']"

[[test.results]]
value = "[d'2012-01-01T00:00:00Z']"

[[test.results]]
value = "['2012-01-01T08:00:00Z-test']"

*/
SELECT * FROM "2012-01-01";
SELECT * FROM <datetime> "2012-01-01";
SELECT * FROM <string> d"2012-01-01T08:00:00Z" + "-test";
