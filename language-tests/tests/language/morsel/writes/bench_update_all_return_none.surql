/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "the statement of the bench update_all_100k_return_none over a table of 2000 records answers as the serial run does, whatever width its compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2000 }]"

[[test.results]]
value = "['n1 hi!', 'n2 hi!', 'n3 hi!']"

*/

DEFINE TABLE record SCHEMALESS;
FOR $i IN 1..2001 { CREATE type::record('record', $i) SET n = $i, name = 'n' + <string> $i RETURN NONE; };
UPDATE record SET name = name + ' hi!' RETURN NONE;
SELECT count() FROM record WHERE string::ends_with(name, ' hi!') GROUP ALL;
SELECT VALUE name FROM record:1..4;
