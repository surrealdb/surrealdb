/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A DEFINE PARAM or REMOVE PARAM in a block changes what the name reads for the block's later statements, in a predicate, a nested subquery and a FROM source"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 7, id: t:7 }]"

[[test.results]]
value = "[{ a: 1, id: u:1 }]"

[[test.results]]
value = "[{ a: 7, id: u:7 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[u:7]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[u:1]"

[[test.results]]
value = "[u:7]"
*/
DEFINE PARAM $target VALUE 1;
CREATE t:1 SET a = 1;
CREATE t:7 SET a = 7;
CREATE u:1 SET a = 1;
CREATE u:7 SET a = 7;
-- The DELETE reads the value the block just defined, so it removes t:7.
{ DEFINE PARAM OVERWRITE $target VALUE 7; DELETE t WHERE id IN (SELECT VALUE id FROM t WHERE a = $target); };
SELECT VALUE id FROM t;
{ DEFINE PARAM $p VALUE 7; RETURN SELECT VALUE id FROM u WHERE a = $p; };
{ REMOVE PARAM $p; RETURN SELECT VALUE id FROM u WHERE a = $p; };
{ DEFINE PARAM $src VALUE u:1; RETURN SELECT VALUE id FROM $src; };
{ DEFINE PARAM OVERWRITE $src VALUE u:7; RETURN SELECT VALUE id FROM $src; };
