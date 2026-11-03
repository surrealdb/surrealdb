/**
[env]
namespace = true
database = true

[test]
reason = "A field typed through an alias of a union, a domain of one that may be NONE or NULL, or a chain of them, orders, compares and takes MIN and MAX by the union's declaration order"

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
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "['sad', 'ok', 'happy']"

[[test.results]]
value = "['ok', 'happy']"

[[test.results]]
value = "['ok', 'sad']"

[[test.results]]
value = "['sad', 'ok']"

[[test.results]]
value = "[{ hi: 'happy', lo: 'sad', top: 'happy' }]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1, t:3]"

*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TYPE feeling AS mood;
DEFINE TYPE maybe_mood AS none | null | mood;
DEFINE TYPE again AS feeling;
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD f ON t TYPE feeling;
DEFINE FIELD m ON t TYPE maybe_mood;
DEFINE FIELD a ON t TYPE option<again>;
CREATE t:1 SET f = 'happy', m = 'happy', a = 'happy' RETURN NONE;
CREATE t:2 SET f = 'sad', m = 'sad', a = 'sad' RETURN NONE;
CREATE t:3 SET f = 'ok', m = 'ok', a = 'ok' RETURN NONE;
SELECT VALUE f FROM t ORDER BY f;
SELECT VALUE m FROM t WHERE m > 'sad' ORDER BY m;
SELECT VALUE a FROM t WHERE a < 'happy' ORDER BY a DESC;
SELECT VALUE f FROM t WHERE f IN 'sad'..='ok' ORDER BY f;
SELECT math::min(f) AS lo, math::max(m) AS hi, math::max(a) AS top FROM t GROUP ALL;
SELECT VALUE id FROM t WHERE f < m OR f > 'ok' ORDER BY id;
UPDATE t SET a = NONE WHERE a >= 'ok' RETURN VALUE id;
