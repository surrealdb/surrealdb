/**
[env]
imports = ["upgrade/define/field_computed_import.surql"]

[test]
upgrade = true
reason = "Fields with VALUE (computed), READONLY, and DEFAULT clauses survive upgrade — computed values recompute on update, READONLY rejects modification, and the dependency graph (ComputedDeps) still drives recomputation"

[[test.results]]
match = '''
    string::contains($result.fields.full_name, "VALUE string::concat(name.first, ' ', name.last)")
    && string::contains($result.fields.created_at, "READONLY")
    && string::contains($result.fields.created_at, "DEFAULT d'2025-01-01T00:00:00Z'")
'''

[[test.results]]
value = "[{ created_at: d'2025-01-01T00:00:00Z', full_name: 'Alice Cooper', id: emp:1, name: { first: 'Alice', last: 'Cooper' } }]"

[[test.results]]
error = true

*/
INFO FOR TABLE emp;
-- Update name.last; full_name (computed via VALUE) must recompute.
UPDATE emp:1 SET name.last = 'Cooper';
-- READONLY field must reject modification.
UPDATE emp:1 SET created_at = d'2099-12-31T00:00:00Z';
