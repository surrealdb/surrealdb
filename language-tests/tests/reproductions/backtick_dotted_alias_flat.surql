/**
[test]
reason = "AS aliases must respect the idiom structure parsed by SurrealQL: `AS foo.bar` (multi-part idiom) nests output into `{ foo: { bar: _ } }`, while backtick-quoted `AS \\`foo.bar\\`` (single-part idiom with a dotted name) stays a flat key `'foo.bar'`. Both the projection (`plan_projections` / `plan_projections_fast`) and aggregation (`plan_aggregation` → `alias_output_path`) planner strategies must agree."

[[test.results]]
value = "[{ findingCount: 10, id: scan:1, state: 'test' }]"

[[test.results]]
value = "[{ findingCount: 20, id: scan:2, state: 'temp' }]"

[[test.results]]
value = "[{ id: scan_definition:test, lastScan: scan:1, lastSuccessfulScan: scan:2 }]"

# Case A: unquoted dotted aliases -> nested output.
[[test.results]]
value = "[{ id: scan_definition:test, lastScan: { findingCount: 10, id: scan:1, state: 'test' }, lastSuccessfulScan: { findingCount: 20, id: scan:2, state: 'temp' } }]"

# Case B: backtick-quoted dotted aliases -> flat keys with dots in the name.
[[test.results]]
value = "[{ id: scan_definition:test, lastScan: scan:1, 'lastScan.findingCount': 10, 'lastScan.id': scan:1, 'lastScan.state': 'test', lastSuccessfulScan: scan:2, 'lastSuccessfulScan.findingCount': 20, 'lastSuccessfulScan.id': scan:2, 'lastSuccessfulScan.state': 'temp' }]"

# Case C: unquoted dotted aliases nest, simple aliased field stays a flat key.
[[test.results]]
value = "[{ asd: NONE, id: scan_definition:test, lastScan: { findingCount: 10, id: scan:1, state: 'test' }, lastSuccessfulScan: { findingCount: 20, id: scan:2, state: 'temp' } }]"

# Case D: backtick-quoted aliases stay flat, unaliased `tester.url` (multi-part
# idiom) nests naturally into `tester: { url: NONE }`.
[[test.results]]
value = "[{ id: scan_definition:test, lastScan: scan:1, 'lastScan.findingCount': 10, 'lastScan.id': scan:1, 'lastScan.state': 'test', lastSuccessfulScan: scan:2, 'lastSuccessfulScan.findingCount': 20, 'lastSuccessfulScan.id': scan:2, 'lastSuccessfulScan.state': 'temp', tester: { url: NONE } }]"

# Setup for GROUP BY cases: duplicate `state = 'test'` so groups have >1 row.
[[test.results]]
value = "[{ findingCount: 30, id: scan:3, state: 'test' }]"

# Case E: backtick-quoted dotted alias with GROUP BY -> flat key. Exercises the
# aggregation planner path (`plan_aggregation` -> `alias_output_path` ->
# `AggregateField::with_output_path`), complementing the projection-only
# coverage above.
[[test.results]]
value = "[{ 'my.state': 'temp', total: 1 }, { 'my.state': 'test', total: 2 }]"

# Case F: unquoted dotted alias with GROUP BY -> nested output. Confirms the
# aggregation planner nests multi-part aliases the same way the projection
# planner does.
[[test.results]]
value = "[{ my: { state: 'temp' }, total: 1 }, { my: { state: 'test' }, total: 2 }]"
*/

UPSERT scan:1 CONTENT { findingCount: 10, state: 'test' };
UPSERT scan:2 CONTENT { findingCount: 20, state: 'temp' };
UPSERT scan_definition:test SET lastScan = scan:1, lastSuccessfulScan = scan:2;

-- Case A: unquoted dotted aliases (multi-part idioms) -> nested output.
SELECT *,
    lastScan.id AS lastScan.id,
    lastScan.findingCount AS lastScan.findingCount,
    lastScan.state AS lastScan.state,
    lastSuccessfulScan.id AS lastSuccessfulScan.id,
    lastSuccessfulScan.findingCount AS lastSuccessfulScan.findingCount,
    lastSuccessfulScan.state AS lastSuccessfulScan.state
FROM scan_definition;

-- Case B: backtick-quoted dotted aliases (single-part idioms) -> flat keys.
SELECT *,
    lastScan.id AS `lastScan.id`,
    lastScan.findingCount AS `lastScan.findingCount`,
    lastScan.state AS `lastScan.state`,
    lastSuccessfulScan.id AS `lastSuccessfulScan.id`,
    lastSuccessfulScan.findingCount AS `lastSuccessfulScan.findingCount`,
    lastSuccessfulScan.state AS `lastSuccessfulScan.state`
FROM scan_definition;

-- Case C: unquoted dotted aliases plus a simple aliased field (`asd`).
SELECT *,
    lastScan.id AS lastScan.id,
    lastScan.findingCount AS lastScan.findingCount,
    lastScan.state AS lastScan.state,
    lastSuccessfulScan.id AS lastSuccessfulScan.id,
    lastSuccessfulScan.findingCount AS lastSuccessfulScan.findingCount,
    lastSuccessfulScan.state AS lastSuccessfulScan.state,
    tester.url AS asd
FROM scan_definition;

-- Case D: backtick-quoted aliases (flat) plus an unaliased multi-part idiom.
SELECT *,
    lastScan.id AS `lastScan.id`,
    lastScan.findingCount AS `lastScan.findingCount`,
    lastScan.state AS `lastScan.state`,
    lastSuccessfulScan.id AS `lastSuccessfulScan.id`,
    lastSuccessfulScan.findingCount AS `lastSuccessfulScan.findingCount`,
    lastSuccessfulScan.state AS `lastSuccessfulScan.state`,
    tester.url
FROM scan_definition;

-- Setup a duplicate state='test' row so GROUP BY has >1 row per group.
UPSERT scan:3 CONTENT { findingCount: 30, state: 'test' };

-- Case E: backtick-quoted dotted alias with GROUP BY (single Part::Field with a
-- dotted name) -> flat key `'my.state'`. Exercises the aggregation planner
-- path which was previously uncovered.
SELECT state AS `my.state`, count() AS total FROM scan GROUP BY state;

-- Case F: unquoted dotted alias with GROUP BY (multi-part idiom [Field(my),
-- Field(state)]) -> nested output `{ my: { state: _ } }`. Confirms the
-- aggregation planner nests multi-part aliases the same way projection does.
SELECT state AS my.state, count() AS total FROM scan GROUP BY state;
