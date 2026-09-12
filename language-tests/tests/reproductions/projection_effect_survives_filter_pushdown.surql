/**
[env]
namespace = true
database = true

[test]
reason = "a predicate over a projected field moves beneath the projection, which must not reduce how often a projected field that writes or can raise is evaluated"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ a: 3, id: t:3 }]"

[[test.results]]
value = "[{ a: 1, w: 1 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ a: 1, bad: '1', id: r:1 }]"

[[test.results]]
value = "[{ a: 2, bad: 'x', id: r:2 }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"
*/

-- `a` is a plain projected field, so `a = 1` may move beneath the inner
-- projection and reach the scan. Beneath it, though, the projection only
-- evaluates for the surviving row, so `fn::writer(a)` would write once
-- instead of once per inner row. A projection holding a field that writes
-- keeps every predicate above it.

DEFINE FUNCTION fn::writer($v: any) { CREATE counter SET n = $v; RETURN $v; };

CREATE t:1 SET a = 1;
CREATE t:2 SET a = 2;
CREATE t:3 SET a = 3;

SELECT * FROM (SELECT a, fn::writer(a) AS w FROM t) WHERE a = 1;

SELECT count() FROM counter GROUP ALL;

-- The same holds for a field that raises rather than writes. Only `r:2`
-- casts badly, and the outer predicate discards it, so pushing the predicate
-- beneath the projection would return a row where the statement as written
-- reports the cast.
CREATE r:1 SET a = 1, bad = '1';
CREATE r:2 SET a = 2, bad = 'x';

SELECT * FROM (SELECT a, <int> bad AS c FROM r) WHERE a = 1;
