/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
`.*` and `.{ … }` fetch a record id just as a field read does — the variants
say so, and `exec::parts::array_ops` and `exec::parts::destructure` are where
it happens. The fetch derives the record's COMPUTED fields and judges its
PERMISSIONS, so it is an evaluation the statement owes: it may raise where
nothing under the step can, and it need not answer the same twice. No rule may
therefore move a filter above one below it, duplicate one by substitution, or
copy one by inlining a function body that reads its argument twice.
"""

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { predicate: 'a = 1' }, children: [{ attributes: { projections: 'a, expanded' }, children: [{ attributes: { fields: 'expanded = link.*' }, children: [{ attributes: { direction: 'Forward', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'expanded', sql: 'link.*' }], operator: 'Compute' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', expressions: [{ role: 'predicate', sql: 'a = 1' }], operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { predicate: 'x.coin' }, children: [{ attributes: { projections: 'x' }, children: [{ attributes: { fields: 'x = link.{ coin }' }, children: [{ attributes: { direction: 'Forward', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'x', sql: 'link.{ coin }' }], operator: 'Compute' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', expressions: [{ role: 'predicate', sql: 'x.coin' }], operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "{ attributes: { expr: 'fn::pair(link.*)' }, children: [{ attributes: { record_id: 't:one' }, context: 'Db', expressions: [{ role: 'record_id', sql: 't:one' }], operator: 'RecordIdScan' }], context: 'Db', expressions: [{ role: 'expr', sql: 'fn::pair(link.*)' }], operator: 'ProjectValue' }"
*/

DEFINE TABLE cfg SCHEMALESS;
DEFINE TABLE t SCHEMALESS;
CREATE cfg:one RETURN NONE;
CREATE t:one SET a = 1, link = cfg:one RETURN NONE;
CREATE t:two SET a = 2, link = cfg:one RETURN NONE;
DEFINE FIELD bad ON cfg COMPUTED <int> 'x';
DEFINE FIELD coin ON cfg COMPUTED rand::bool();
DEFINE FUNCTION fn::pair($x: any) { RETURN [$x, $x]; };

-- The expansion raises when it is reached, which is what makes it an
-- evaluation rather than a read.
SELECT VALUE link.* FROM t:one;
SELECT VALUE link.{ bad } FROM t:one;

-- A filter over a sibling column may not move below the projection and skip
-- the expansion: `Filter [predicate: a = 1]` stands above `SelectProject`.
SELECT * FROM (SELECT a, link.* AS expanded FROM t) WHERE a = 1 EXPLAIN;

-- The destructure is not substituted into the predicate above it, which would
-- leave two fetches where the statement owes one.
SELECT * FROM (SELECT link.{ coin } AS x FROM t) WHERE x.coin EXPLAIN;

-- Inlining a body that reads its argument twice would turn one fetch into
-- two, so the call is left standing.
SELECT VALUE fn::pair(link.*) FROM t:one EXPLAIN;
