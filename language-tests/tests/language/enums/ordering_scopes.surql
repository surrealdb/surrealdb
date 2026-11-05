/**
[env]
namespace = true
database = true

[test]
reason = "Declaration-order ordering reaches an enum field read from several tables that declare the same enum, and from a subquery's own table, and an unrelated string field of another table keeps the order of its spelling"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "['sad', 'ok', 'happy', 'happy']"

[[test.results]]
value = "['happy', 'happy']"

[[test.results]]
value = "['happy', 'ok', 'sad']"

[[test.results]]
value = "['happy']"
*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE a SCHEMAFULL;
DEFINE FIELD m ON a TYPE mood;
DEFINE TABLE b SCHEMAFULL;
DEFINE FIELD m ON b TYPE mood;
DEFINE TABLE c SCHEMAFULL;
DEFINE FIELD m ON c TYPE string;
CREATE a:1 SET m = 'happy' RETURN NONE;
CREATE a:2 SET m = 'sad' RETURN NONE;
CREATE b:1 SET m = 'ok' RETURN NONE;
CREATE b:2 SET m = 'happy' RETURN NONE;
CREATE c:1 SET m = 'ok' RETURN NONE;
SELECT VALUE m FROM a, b ORDER BY m;
SELECT VALUE m FROM a, b WHERE m > 'ok' ORDER BY m;
SELECT VALUE m FROM a, c ORDER BY m;
SELECT VALUE m FROM a WHERE id IN (SELECT VALUE id FROM a WHERE m > 'sad') ORDER BY m;
