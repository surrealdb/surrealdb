/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "the statement of the bench delete_all_100k over a table of 2000 records answers as the serial run does, whatever width its compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

*/

DEFINE TABLE record SCHEMALESS;
FOR $i IN 1..2001 { CREATE type::record('record', $i) SET n = $i RETURN NONE; };
DELETE record RETURN NONE;
SELECT count() FROM record GROUP ALL;
