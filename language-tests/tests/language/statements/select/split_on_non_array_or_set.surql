/**

[test]

[[test.results]]
value = "[{ a: 1, b: 'str', c: d'2026-01-17T00:00:00Z', d: 1..3, e: { k1: 'v1', k2: 'v2' }, id: t:a }]"

[[test.results]]
value = "[{ a: 1, id: t:a }]"

[[test.results]]
value = "[{ b: 'str', id: t:a }]"

[[test.results]]
value = "[{ c: d'2026-01-17T00:00:00Z', id: t:a }]"

[[test.results]]
value = "[{ d: 1..3, id: t:a }]"

[[test.results]]
value = "[{ e: { k1: 'v1', k2: 'v2' }, id: t:a }]"

[[test.results]]
value = "[{ a: 1, b: 'str', id: t:a }]"

[[test.results]]
value = "[{ a: 1, b: 'str', c: d'2026-01-17T00:00:00Z', id: t:a }]"

[[test.results]]
value = "[{ a: 1, b: 'str', c: d'2026-01-17T00:00:00Z', d: 1..3, id: t:a }]"
*/

CREATE t:a SET a = 1, b = "str", c = d'2026-01-17T00:00:00Z', d = 1..3, e = { k1: "v1", k2: "v2" };

SELECT id, a FROM t SPLIT a;

SELECT id, b FROM t SPLIT b;

SELECT id, c FROM t SPLIT c;

SELECT id, d FROM t SPLIT d;

SELECT id, e FROM t SPLIT e;

SELECT id, a, b FROM t SPLIT a, b;

SELECT id, a, b, c FROM t SPLIT a, b, c;

SELECT id, a, b, c, d FROM t SPLIT a, b, c, d;
