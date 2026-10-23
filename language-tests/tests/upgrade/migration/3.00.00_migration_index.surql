/**
[env]
imports = ["upgrade/migration/3.00.00_migration_index_import.surql"]

[test]
upgrade = true
migration = true
reason = "A standard and a unique index written to RocksDB keep answering lookups after the directory is migrated to SurrealKV."

[[test.results]]
value = "[{ email: 'a@example.com', id: mig_doc:1, org: 'surrealdb' }, { email: 'b@example.com', id: mig_doc:2, org: 'surrealdb' }]"

[[test.results]]
value = "[{ email: 'c@example.com', id: mig_doc:3, org: 'other' }]"

[[test.results]]
value = "[{ email: 'b@example.com', id: mig_doc:2, org: 'surrealdb' }]"

*/
SELECT * FROM mig_doc WITH INDEX mig_idx_org WHERE org = 'surrealdb' ORDER BY id;
SELECT * FROM mig_doc WITH INDEX mig_idx_org WHERE org = 'other' ORDER BY id;
SELECT * FROM mig_doc WITH INDEX mig_idx_email WHERE email = 'b@example.com';
