/**
[env]
imports = ["upgrade/define/3.00.00_reference_strategies_import.surql"]

[test]
upgrade = true
reason = """
Covers the durable `StoredReferenceDeleteStrategy` written by a REFERENCE
field's ON DELETE clause: Reject, Ignore, Cascade and the custom THEN form,
including the SurrealQL text the custom variant carries.

Each strategy is exercised, not just rendered: the new binary deletes the
referenced record and the referencing row must be rejected, left alone, deleted
or rewritten by the custom clause exactly as the stored variant says. A DDL
round-trip alone passes even when a strategy decodes to the wrong variant,
because every variant renders from the same enum it decoded into.
"""

[[test.results]]
match = '''string::contains($result.fields.target, "REFERENCE ON DELETE REJECT")'''

[[test.results]]
match = '''string::contains($result.fields.target, "REFERENCE ON DELETE IGNORE")'''

[[test.results]]
match = '''string::contains($result.fields.target, "REFERENCE ON DELETE CASCADE")'''

[[test.results]]
match = '''
    string::contains($result.fields.target, "REFERENCE ON DELETE THEN")
        && string::contains($result.fields.target, "UPDATE ONLY $this SET orphaned = $reference")
'''

[[test.results]]
error = "Cannot delete `up_reference_strategies_target:reject` as it is referenced by `up_reference_strategies_reject:1` with an ON DELETE REJECT clause"

[[test.results]]
value = "[up_reference_strategies_target:reject]"

[[test.results]]
value = "[{ id: up_reference_strategies_reject:1, target: up_reference_strategies_target:reject }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: up_reference_strategies_ignore:1, target: up_reference_strategies_target:ignore }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: up_reference_strategies_custom:1, orphaned: up_reference_strategies_target:custom }]"

*/
INFO FOR TABLE up_reference_strategies_reject;
INFO FOR TABLE up_reference_strategies_ignore;
INFO FOR TABLE up_reference_strategies_cascade;
INFO FOR TABLE up_reference_strategies_custom;

-- REJECT: the delete is refused while the referencing row lives, and both
-- records are left exactly as the old binary wrote them.
DELETE up_reference_strategies_target:reject;
SELECT VALUE id FROM up_reference_strategies_target:reject;
SELECT id, target FROM up_reference_strategies_reject;

-- IGNORE: the delete goes through and the referencing field keeps pointing at
-- the record that no longer exists.
DELETE up_reference_strategies_target:ignore;
SELECT id, target FROM up_reference_strategies_ignore;

-- CASCADE: the delete takes the referencing row with it.
DELETE up_reference_strategies_target:cascade;
SELECT VALUE id FROM up_reference_strategies_cascade;

-- THEN: the stored expression runs against the referencing row, with $this
-- bound to that row and $reference to the record being deleted.
DELETE up_reference_strategies_target:custom;
SELECT id, orphaned FROM up_reference_strategies_custom;
