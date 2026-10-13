/**
[env]
planner-strategy = ["compute-only"]

[test]
reason = "SDB-207: pins classic-planner behavior for AND + OR-residual conditions. The indexable AND prefix (scanDefinition, createdAt) should use the compound index with the OR clause as a per-record filter, but any OR in the condition currently forces a full table scan. Update the EXPLAIN expectations when the planner learns to use the conjunct prefix."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ createdAt: d'2026-01-10T00:00:00Z', id: scan:one, scanDefinition: scan_definition:a, task: { finishedAt: d'2026-01-11T00:00:00Z' } }]"

[[test.results]]
value = "[{ createdAt: d'2026-01-01T00:00:00Z', id: canonical_finding:f1, scanDefinition: scan_definition:a }]"

[[test.results]]
value = "[{ closedAt: d'2026-01-12T00:00:00Z', createdAt: d'2026-01-02T00:00:00Z', id: canonical_finding:f2, scanDefinition: scan_definition:a }]"

[[test.results]]
value = "[{ closedAt: d'2026-01-05T00:00:00Z', createdAt: d'2026-01-03T00:00:00Z', id: canonical_finding:f3, scanDefinition: scan_definition:a }]"

[[test.results]]
value = "[{ createdAt: d'2026-01-01T00:00:00Z', id: canonical_finding:f4, scanDefinition: scan_definition:b }]"

[[test.results]]
value = "[{ createdAt: d'2026-02-01T00:00:00Z', id: canonical_finding:f5, scanDefinition: scan_definition:a }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'canonical_finding' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: canonical_finding:f1 }, { id: canonical_finding:f2 }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'canonical_finding' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: canonical_finding:f1 }, { id: canonical_finding:f2 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'scanDefinition_compound_idx', prefix: [scan_definition:a], ranges: [{ operator: '<=', value: d'2026-01-11T00:00:00Z' }] }, table: 'canonical_finding' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: canonical_finding:f1 }, { id: canonical_finding:f2 }, { id: canonical_finding:f3 }]"
*/

DEFINE INDEX scanDefinition_compound_idx ON TABLE canonical_finding COLUMNS scanDefinition, createdAt, closedAt;
CREATE scan:one SET scanDefinition = scan_definition:a, createdAt = d'2026-01-10T00:00:00Z', task = { finishedAt: d'2026-01-11T00:00:00Z' };
CREATE canonical_finding:f1 SET scanDefinition = scan_definition:a, createdAt = d'2026-01-01T00:00:00Z';
CREATE canonical_finding:f2 SET scanDefinition = scan_definition:a, createdAt = d'2026-01-02T00:00:00Z', closedAt = d'2026-01-12T00:00:00Z';
CREATE canonical_finding:f3 SET scanDefinition = scan_definition:a, createdAt = d'2026-01-03T00:00:00Z', closedAt = d'2026-01-05T00:00:00Z';
CREATE canonical_finding:f4 SET scanDefinition = scan_definition:b, createdAt = d'2026-01-01T00:00:00Z';
CREATE canonical_finding:f5 SET scanDefinition = scan_definition:a, createdAt = d'2026-02-01T00:00:00Z';

LET $scan = scan:one;

-- Customer query shape: param-idiom operands
SELECT id FROM canonical_finding WHERE scanDefinition = $scan.scanDefinition AND createdAt <= $scan.task.finishedAt AND (closedAt IS NONE OR closedAt >= $scan.createdAt) EXPLAIN;
SELECT id FROM canonical_finding WHERE scanDefinition = $scan.scanDefinition AND createdAt <= $scan.task.finishedAt AND (closedAt IS NONE OR closedAt >= $scan.createdAt);

-- Same shape with literal operands (isolates param resolution from AND/OR planning)
SELECT id FROM canonical_finding WHERE scanDefinition = scan_definition:a AND createdAt <= d'2026-01-11T00:00:00Z' AND (closedAt IS NONE OR closedAt >= d'2026-01-10T00:00:00Z') EXPLAIN;
SELECT id FROM canonical_finding WHERE scanDefinition = scan_definition:a AND createdAt <= d'2026-01-11T00:00:00Z' AND (closedAt IS NONE OR closedAt >= d'2026-01-10T00:00:00Z');

-- Control: pure AND (no OR) — this is the part the planner should already index
SELECT id FROM canonical_finding WHERE scanDefinition = scan_definition:a AND createdAt <= d'2026-01-11T00:00:00Z' EXPLAIN;
SELECT id FROM canonical_finding WHERE scanDefinition = scan_definition:a AND createdAt <= d'2026-01-11T00:00:00Z';
