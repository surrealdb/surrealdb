/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
A field read off a value that may be a record link fetches that record, which
derives its COMPUTED fields and judges its PERMISSIONS. That is an evaluation
the statement owes, and it need not answer the same twice, so no rule may drop
it, read it once for many rows, or duplicate it.
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
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { sort_keys: 'link.bad ASC' }, children: [{ attributes: { record_id: 't:one' }, context: 'Db', expressions: [{ role: 'record_id', sql: 't:one' }], operator: 'RecordIdScan' }], context: 'Db', operator: 'SortByKey' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: 'k INSIDE (ProjectValue)', table: 'outr' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'expr', sql: 'id' }], operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { predicate: 'x' }, children: [{ attributes: { projections: 'x' }, children: [{ attributes: { fields: 'x = link.coin' }, children: [{ attributes: { direction: 'Forward', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'x', sql: 'link.coin' }], operator: 'Compute' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', expressions: [{ role: 'predicate', sql: 'x' }], operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"


*/

DEFINE TABLE cfg SCHEMALESS;
DEFINE TABLE t SCHEMALESS;
DEFINE TABLE r SCHEMALESS;
DEFINE TABLE outr SCHEMALESS;
CREATE cfg:one RETURN NONE;
CREATE t:one SET link = cfg:one RETURN NONE;
CREATE r:1 SET k = 1, link = cfg:one RETURN NONE;
CREATE outr:1 SET k = 1 RETURN NONE;
DEFINE FIELD bad ON cfg COMPUTED <int> 'x';
DEFINE FIELD coin ON cfg COMPUTED rand::bool();

-- A sort over one row keeps its Sort: the key may fetch, and a rule that
-- dropped it would drop that evaluation. `SortByKey` extracts the key by path
-- rather than evaluating it, so the fetch does not happen there and no error
-- surfaces; the plan is what this pins.
SELECT id FROM t:one ORDER BY link.bad EXPLAIN;

-- Evaluated rather than extracted, the same path raises.
SELECT VALUE link.bad FROM t:one;

-- The subquery's right side is not read once for every outer row.
SELECT VALUE id FROM outr WHERE k IN (SELECT VALUE k FROM r WHERE link.coin) EXPLAIN;

-- The projection's path is not duplicated into the predicate above it.
SELECT * FROM (SELECT link.coin AS x FROM t) WHERE x EXPLAIN;
