/**
[env]
namespace = true
database = true

[test]
reason = """
A membership subquery whose right side dereferences a record id is not
decorrelated into a join. The join reads its right side once for every outer
row, and the dereference fetches the record — deriving its COMPUTED fields —
so a body that draws entropy answers afresh per evaluation. Reading it once
would decide every row together, which the scalar subquery does not do.
"""

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
value = "[{ id: r:1, k: 1 }]"

[[test.results]]
value = "[{ id: outr:1, k: 1 }]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k INSIDE (subquery)' }, children: [{ attributes: { direction: 'Forward', table: 'outr' }, context: 'Db', operator: 'TableScan' }, { attributes: { strategy: 'apply' }, children: [{ attributes: { expr: 'k' }, children: [{ attributes: { predicate: '(cfg:one).coin' }, children: [{ attributes: { direction: 'Forward', table: 'r' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }], context: 'Db', operator: 'Subquery' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k INSIDE (subquery)' }, children: [{ attributes: { direction: 'Forward', table: 'outr' }, context: 'Db', operator: 'TableScan' }, { attributes: { strategy: 'once' }, children: [{ attributes: { expr: 'k' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'yes', predicate: 'k > 0', table: 'r' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'ProjectValue' }], context: 'Db', operator: 'Subquery' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"


*/

DEFINE TABLE cfg SCHEMALESS;
DEFINE FIELD coin ON cfg COMPUTED rand::bool();
DEFINE TABLE r SCHEMALESS;
DEFINE TABLE outr SCHEMALESS;
CREATE cfg:one RETURN NONE;
CREATE r:1 SET k = 1;
CREATE outr:1 SET k = 1;

-- The right side dereferences `cfg:one`: no Join.
SELECT VALUE id FROM outr WHERE k IN (SELECT VALUE k FROM r WHERE (cfg:one).coin) EXPLAIN;

-- The same shape with a right side that reads only its own row: a Join.
SELECT VALUE id FROM outr WHERE k IN (SELECT VALUE k FROM r WHERE k > 0) EXPLAIN;
