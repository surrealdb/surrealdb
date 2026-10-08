/**
[env]
namespace = true
database = true

[test]
reason = """
`time::nano` and its family take an optional datetime and read the clock when
they do not get one, so a call that takes the default is a clock reading and
two of them disagree. Reported deterministic, such a call was inert to the
rules: an uncorrelated membership subquery holding one could become a join,
whose right side is built once and shared by every probe, where the scalar
form reads the clock again for each outer row. The default is what makes them
volatile, and an argument that evaluates to NONE takes it just as an omitted
one does, so the whole family is registered non-deterministic and the subquery
stays a subquery.
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
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k INSIDE (subquery)' }, children: [{ attributes: { direction: 'Forward', table: 'outr' }, context: 'Db', operator: 'TableScan' }, { attributes: { strategy: 'apply' }, children: [{ attributes: { expr: 'k' }, children: [{ attributes: { predicate: 'time::nano((IF k = k THEN NONE END)) % 2 = 0' }, children: [{ attributes: { direction: 'Forward', table: 'r' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }], context: 'Db', operator: 'Subquery' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = """{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k INSIDE (subquery)' }, children: [{ attributes: { direction: 'Forward', table: 'outr' }, context: 'Db', operator: 'TableScan' }, { attributes: { strategy: 'apply' }, children: [{ attributes: { expr: 'k' }, children: [{ attributes: { predicate: "time::year(d'2024-01-01T00:00:00Z') = 2024" }, children: [{ attributes: { direction: 'Forward', table: 'r' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }], context: 'Db', operator: 'Subquery' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"""
*/

DEFINE TABLE outr SCHEMALESS;
DEFINE TABLE r SCHEMALESS;
CREATE outr:1 SET k = 1 RETURN NONE;
CREATE r:1 SET k = 1 RETURN NONE;

-- The argument is written, but evaluates to NONE, so the call still defaults
-- to the clock; nothing about the call site says so.
SELECT VALUE id FROM outr WHERE k IN (SELECT VALUE k FROM r WHERE time::nano(IF k = k THEN NONE END) % 2 = 0) EXPLAIN;

-- Given a datetime the same call is a plain field read, but the flag is a
-- property of the name, so this subquery stays a subquery too.
SELECT VALUE id FROM outr WHERE k IN (SELECT VALUE k FROM r WHERE time::year(d"2024-01-01T00:00:00Z") = 2024) EXPLAIN;
