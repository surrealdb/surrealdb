/**
[env]
namespace = true
database = true

[test]
reason = """
	A bound that is not a record id of the scanned table with a number, string or uuid key bounds
	nothing: another table, a value that is no record id, `NONE`, `NULL`, and an array, object,
	range or generated key all leave the predicate to judge every row, and it answers as a
	comparison of values does.
"""

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[[test.results]]
value = "[{ id: t:2, n: 2 }]"

[[test.results]]
value = "[{ id: t:a, n: 'a' }]"

[[test.results]]
value = "[{ id: t:[1, 2], n: 'array' }]"

[[test.results]]
value = "[{ id: t:{ a: 1 }, n: 'object' }]"

[[test.results]]
value = "[{ id: u:1, n: 'other' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1, t:2, t:a, t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:1, t:2, t:a, t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1, t:2, t:a, t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1, t:2, t:a, t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:1, t:2, t:a]"

[[test.results]]
value = "[t:{ a: 1 }]"

[[test.results]]
value = "[t:{ a: 1 }]"

[[test.results]]
value = "[t:{ a: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1, t:2, t:a, t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1, t:2, t:a, t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1, t:2, t:a, t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:2, t:a, t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[]"
*/

CREATE t:1 SET n = 1;
CREATE t:2 SET n = 2;
CREATE t:a SET n = 'a';
CREATE t:[1, 2] SET n = 'array';
CREATE t:{ a: 1 } SET n = 'object';
CREATE u:1 SET n = 'other';
SELECT VALUE id FROM t WHERE id > u:1;
SELECT VALUE id FROM t WHERE id < u:1;
SELECT VALUE id FROM t WHERE id >= s:1;
SELECT VALUE id FROM t WHERE id <= s:1;
SELECT VALUE id FROM t WHERE id > 5;
SELECT VALUE id FROM t WHERE id < 5;
SELECT VALUE id FROM t WHERE id > 'a';
SELECT VALUE id FROM t WHERE id < 'a';
SELECT VALUE id FROM t WHERE id > t:[1];
SELECT VALUE id FROM t WHERE id < t:[1];
SELECT VALUE id FROM t WHERE id > t:[1, 2];
SELECT VALUE id FROM t WHERE id >= t:{ a: 1 };
SELECT VALUE id FROM t WHERE id > t:{ a: 0 };
SELECT VALUE id FROM t WHERE id > t:1..5;
SELECT VALUE id FROM t WHERE id > NONE;
SELECT VALUE id FROM t WHERE id < NONE;
SELECT VALUE id FROM t WHERE id > NULL;
SELECT VALUE id FROM t WHERE id < NULL;
SELECT VALUE id FROM t WHERE id > true;
-- A side that bounds nothing leaves the other standing.
SELECT VALUE id FROM t WHERE id > 5 AND id < t:2;
SELECT VALUE id FROM t WHERE id > t:1 AND id < u:9;
SELECT VALUE id FROM t WHERE id > u:1 AND id < t:2;
