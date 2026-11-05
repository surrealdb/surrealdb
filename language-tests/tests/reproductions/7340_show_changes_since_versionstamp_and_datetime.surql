/**
[env]
namespace = true
database = true

[test]
requires = ["changefeed"]
reason = "A changefeed read by versionstamp or by a datetime after its creation returns the writes made since (issues 7340 and 7433)"
issue = 7340

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: r:1, x: 1 }]"

[[test.results]]
match = "array::len($result) == 2"

[[test.results]]
match = "array::len($result) == 2"

[[test.results]]
value = "[]"
*/
DEFINE TABLE r CHANGEFEED 1d;
CREATE r:1 SET x = 1;
SHOW CHANGES FOR TABLE r SINCE 1 LIMIT 10;
SHOW CHANGES FOR TABLE r SINCE d'2020-01-01T00:00:00Z' LIMIT 10;
SHOW CHANGES FOR TABLE r SINCE d'2099-01-01T00:00:00Z' LIMIT 10;
