/**
[env]
namespace = true
database = true

[test]
reason = "Two fields of different enums share no declaration order, so comparing them compares their labels as written rather than ranking one side by the other's enum"

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
value = "[t:1]"

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "[]"
*/

DEFINE TYPE left AS 'z' | 'a';
DEFINE TYPE right AS 'b' | 'y';
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD l ON t TYPE left;
DEFINE FIELD r ON t TYPE right;
CREATE t:1 SET l = 'a', r = 'y' RETURN NONE;
CREATE t:2 SET l = 'z', r = 'b' RETURN NONE;
SELECT VALUE id FROM t WHERE l < r ORDER BY id;
SELECT VALUE id FROM t WHERE l > r ORDER BY id;
SELECT VALUE id FROM t WHERE l < 'z' ORDER BY id;
