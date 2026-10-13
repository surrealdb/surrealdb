/**
[test]
reason = "SDB-207 differential companion: rows-only assertions with no planner-strategy, so every engine (compute-only, all-ro, best-effort-ro) must agree on results regardless of whether the record-idiom fold or the compound index is used"

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
value = "[{ id: canonical_finding:f1 }, { id: canonical_finding:f2 }]"

[[test.results]]
value = "[{ id: canonical_finding:f1 }, { id: canonical_finding:f2 }]"

[[test.results]]
value = "[{ id: canonical_finding:f1 }, { id: canonical_finding:f2 }, { id: canonical_finding:f3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"
*/

DEFINE INDEX scanDefinition_compound_idx ON TABLE canonical_finding COLUMNS scanDefinition, createdAt, closedAt;
CREATE scan:one SET scanDefinition = scan_definition:a, createdAt = d'2026-01-10T00:00:00Z', task = { finishedAt: d'2026-01-11T00:00:00Z' };
CREATE canonical_finding:f1 SET scanDefinition = scan_definition:a, createdAt = d'2026-01-01T00:00:00Z';
CREATE canonical_finding:f2 SET scanDefinition = scan_definition:a, createdAt = d'2026-01-02T00:00:00Z', closedAt = d'2026-01-12T00:00:00Z';
CREATE canonical_finding:f3 SET scanDefinition = scan_definition:a, createdAt = d'2026-01-03T00:00:00Z', closedAt = d'2026-01-05T00:00:00Z';
CREATE canonical_finding:f4 SET scanDefinition = scan_definition:b, createdAt = d'2026-01-01T00:00:00Z';
CREATE canonical_finding:f5 SET scanDefinition = scan_definition:a, createdAt = d'2026-02-01T00:00:00Z';

LET $scan = scan:one;

-- Customer query shape: param-idiom operands with an OR residual
SELECT id FROM canonical_finding WHERE scanDefinition = $scan.scanDefinition AND createdAt <= $scan.task.finishedAt AND (closedAt IS NONE OR closedAt >= $scan.createdAt);

-- Literal operands
SELECT id FROM canonical_finding WHERE scanDefinition = scan_definition:a AND createdAt <= d'2026-01-11T00:00:00Z' AND (closedAt IS NONE OR closedAt >= d'2026-01-10T00:00:00Z');

-- Pure AND (no OR residual)
SELECT id FROM canonical_finding WHERE scanDefinition = scan_definition:a AND createdAt <= d'2026-01-11T00:00:00Z';

-- Traversal through a missing record yields NONE
LET $ghost = scan:nope;
SELECT id FROM canonical_finding WHERE scanDefinition = $ghost.scanDefinition;
