/**
[env]
namespace = true
database = true
compatibility = 3

[test]
reason = """
	A SCHEMAFULL table whose rows are packed, under two layouts and beside a row written as a
	document, holds an int field under a `UNIQUE` index. `math::max` and `math::min` over the whole
	table, which an ordered probe of the index answers from the field of each row it fetches, and
	reads bounded by literal record ids, with an aggregate, a top-k sort or an equality on a field
	the index does not hold above them, answer what the whole decode of each row holds, before and
	after the row that holds the extreme changes.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:100, n: 500, s: 'a document' }]"

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
value = "[{ m: 500 }]"

[[test.results]]
value = "[{ m: -48 }]"

[[test.results]]
value = "500"

[[test.results]]
value = "-48"

[[test.results]]
value = "[{ m: 47 }]"

[[test.results]]
value = "[{ m: -47 }]"

[[test.results]]
value = "[{ m: 47 }]"

[[test.results]]
value = "[{ m: 50 }]"

[[test.results]]
value = "[47, 40, 37]"

[[test.results]]
value = "[{ id: t:52, n: -45 }, { id: t:53, n: -8 }, { id: t:55, n: -35 }, { id: t:58, n: -25 }]"

[[test.results]]
value = "[t:13, t:20, t:27, t:12]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ id: t:41, n: -48 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:41, n: 900 }]"

[[test.results]]
value = "[{ m: 900 }]"

[[test.results]]
value = "[{ m: -47 }]"

[[test.results]]
value = "[{ m: 900 }]"

[[test.results]]
value = "[900, 26]"

[[test.results]]
value = "[{ m: -38 }]"

*/

DEFINE TABLE t SCHEMALESS;
CREATE t:100 CONTENT { n: 500, s: 'a document' };
DEFINE TABLE OVERWRITE t SCHEMAFULL;
DEFINE FIELD n ON t TYPE int;
DEFINE FIELD s ON t TYPE string;
DEFINE FIELD k ON t TYPE int;
FOR $i IN 1..30 { CREATE type::record('t', $i) CONTENT { n: ($i * 37) % 101 - 50, s: 's' + <string> $i, k: $i % 7 } };
DEFINE FIELD note ON t TYPE option<string>;
FOR $i IN 30..60 { CREATE type::record('t', $i) CONTENT { n: ($i * 37) % 101 - 50, s: 's' + <string> $i, k: $i % 7, note: 'g2' } };
DEFINE INDEX n ON t FIELDS n UNIQUE;

-- The extreme over the whole table, answered from the end of the index.
SELECT math::max(n) AS m FROM t GROUP ALL;
SELECT math::min(n) AS m FROM t GROUP ALL;
RETURN (SELECT VALUE math::max(n) FROM t GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM t GROUP ALL)[0];

-- Reads bounded by literal record ids.
SELECT math::max(n) AS m FROM t WHERE id >= t:10 AND id < t:30 GROUP ALL;
SELECT math::min(n) AS m FROM t WHERE id >= t:10 AND id < t:30 GROUP ALL;
SELECT math::max(n) AS m FROM t:10..30 GROUP ALL;
SELECT math::max(n) AS m FROM t:10..=30 GROUP ALL;
SELECT VALUE n FROM t WHERE id >= t:10 AND id < t:30 AND n > 0 ORDER BY n DESC LIMIT 3;
SELECT id, n FROM t WHERE id > t:50 AND id < t:100 AND n < 0 ORDER BY id;
SELECT VALUE id FROM t WHERE id >= t:10 AND id < t:30 AND k > 3 ORDER BY k DESC, id LIMIT 4;
SELECT count() FROM t WHERE id > t:20 AND k = 5 GROUP ALL;

-- The row that holds the extreme changes.
SELECT id, n FROM t WHERE n = -48;
DELETE t:100;
UPDATE t:41 SET n = 900 RETURN id, n;
SELECT math::max(n) AS m FROM t GROUP ALL;
SELECT math::min(n) AS m FROM t GROUP ALL;
SELECT math::max(n) AS m FROM t:40..45 GROUP ALL;
SELECT VALUE n FROM t WHERE id >= t:40 AND id < t:45 ORDER BY n DESC LIMIT 2;
SELECT math::min(n) AS m FROM t WHERE id >= t:40 AND id < t:45 GROUP ALL;
