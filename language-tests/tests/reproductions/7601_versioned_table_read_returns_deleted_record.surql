/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 10000

[test]
reason = "A table read with VERSION must not return a record deleted before that moment, whether or not a predicate or ORDER BY is present"
issue = 7601

[[test.results]]
value = "[{ id: t:a, n: 1 }]"

[[test.results]]
value = "[{ id: t:b, n: 2 }]"

[[test.results]]
value = "[{ id: t:c, n: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:a, n: 10 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:c, n: 3 }, { id: t:a, n: 10 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:c, n: 3 }, { id: t:a, n: 10 }]"

[[test.results]]
value = "[{ id: t:a, n: 1 }, { id: t:b, n: 2 }, { id: t:c, n: 3 }]"

*/

CREATE t:a SET n = 1; CREATE t:b SET n = 2; CREATE t:c SET n = 3;
SLEEP 100ms;
LET $pre = time::now();
SLEEP 100ms;
DELETE t:b; UPDATE t:a SET n = 10;
SLEEP 100ms;
LET $v = time::now();
SELECT id, n FROM t ORDER BY n VERSION $v;
SELECT count() FROM t WHERE n > 0 GROUP ALL VERSION $v;
SELECT count() FROM t GROUP ALL VERSION $v;
SELECT * FROM t:b VERSION $v;
SELECT id, n FROM t ORDER BY n;
SELECT id, n FROM t ORDER BY n VERSION $pre;
