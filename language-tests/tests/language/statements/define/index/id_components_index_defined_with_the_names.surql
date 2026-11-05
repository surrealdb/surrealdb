/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An index defined in the transaction that names the components of the record id
reads those components as that transaction sees them. Its build runs in
transactions of its own, which do not see the transaction's uncommitted
definition of `id`, so the structure of the id is taken from the defining
transaction: a build that compiled the columns without it would read `b` and
`a` as stored fields, which the rows do not hold, and index every row under
NONE. The rows are committed before the transaction, so the build reads them
all. Index-only reads and counts give the rows a scan gives.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: t, condition: a = 'x']
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[1, 2, 3, 4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[2, 4]"

[[test.results]]
value = "[{ count: 2 }]"
*/
DEFINE TABLE t SCHEMALESS;
CREATE t:['x', d'2024-01-01T00:00:00.500Z'] SET v = 1 RETURN NONE;
CREATE t:['x', d'2024-01-01T00:00:01Z'] SET v = 2 RETURN NONE;
CREATE t:['y', d'2024-01-01T00:00:00Z'] SET v = 3 RETURN NONE;
BEGIN;
DEFINE FIELD id ON t TYPE [a: string, b: datetime];
DEFINE INDEX by_b ON t FIELDS b;
DEFINE INDEX by_a_b ON t FIELDS a, b;
DEFINE INDEX x_count ON t COUNT WHERE a = 'x';
COMMIT;
SELECT VALUE v FROM t WITH INDEX by_b WHERE b > d'2023-01-01T00:00:00Z' ORDER BY v;
SELECT VALUE v FROM t WITH INDEX by_a_b WHERE a = 'x' AND b >= d'2024-01-01T00:00:00Z' ORDER BY v;
SELECT VALUE v FROM t WITH NOINDEX WHERE a = 'x' AND b >= d'2024-01-01T00:00:00Z' ORDER BY v;
EXPLAIN SELECT count() FROM t WHERE a = 'x' GROUP ALL;
SELECT count() FROM t WHERE a = 'x' GROUP ALL;
CREATE t SET a = 'x', b = d'2024-01-01T00:00:00.700Z', v = 4 RETURN NONE;
SELECT count() FROM t WHERE a = 'x' GROUP ALL;
SELECT VALUE v FROM t WITH INDEX by_b WHERE b > d'2023-01-01T00:00:00Z' ORDER BY v;
DELETE t WHERE b < d'2024-01-01T00:00:00.600Z' RETURN NONE;
SELECT VALUE v FROM t WITH INDEX by_b WHERE b > d'2023-01-01T00:00:00Z' ORDER BY v;
SELECT count() FROM t WHERE a = 'x' GROUP ALL;
