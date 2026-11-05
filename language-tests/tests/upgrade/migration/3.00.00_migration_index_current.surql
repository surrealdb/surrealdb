/**
[env]
imports = ["upgrade/migration/3.00.00_migration_index_import.surql"]

[test]
upgrade = true
migration = true
reason = "An index that cannot be read after migration falls back to a table scan and returns the same rows, so this asserts that the plans name the migrated indexes, then that writes still maintain them. The plan is gated to the current checkout because EXPLAIN output differs across releases, and the writes sit apart from the reads."
version = ">=3.4.0-a"

[[test.results]]
match = '''string::contains($result, "mig_idx_org")'''

[[test.results]]
match = '''string::contains($result, "mig_idx_email")'''

[[test.results]]
value = "[{ email: 'd@example.com', id: mig_doc:4, org: 'other' }]"

[[test.results]]
value = "[{ id: mig_doc:3 }, { id: mig_doc:4 }]"

[[test.results]]
value = "[{ id: mig_doc:4 }]"

*/
EXPLAIN SELECT * FROM mig_doc WITH INDEX mig_idx_org WHERE org = 'other';
EXPLAIN SELECT * FROM mig_doc WITH INDEX mig_idx_email WHERE email = 'b@example.com';
CREATE mig_doc:4 SET email = 'd@example.com', org = 'other';
SELECT id FROM mig_doc WITH INDEX mig_idx_org WHERE org = 'other' ORDER BY id;
SELECT id FROM mig_doc WITH INDEX mig_idx_email WHERE email = 'd@example.com';
