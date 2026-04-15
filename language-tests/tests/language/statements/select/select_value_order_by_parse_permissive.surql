/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "SELECT VALUE ... ORDER BY is parse-time permissive: ORDER BY need not match the VALUE expression; unknown sort paths sort as NONE (same multiset as when all keys tie)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2]"

*/

{
	DEFINE TABLE OVERWRITE t SCHEMALESS;
	CREATE t:1 SET x = 1;
	CREATE t:2 SET x = 2;
	RETURN "OK";
};

SELECT VALUE x FROM t ORDER BY x ASC;
-- Parses even though `does_not_exist` is not the VALUE expression; sort key is NONE per row
SELECT VALUE x FROM t ORDER BY does_not_exist ASC;
