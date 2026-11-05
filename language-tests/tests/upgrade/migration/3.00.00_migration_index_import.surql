/**
[test]
reason = "Setup for migration_index.surql and migration_index_current.surql: a RocksDB store holding a standard and a unique index."
run = false
*/
DEFINE INDEX mig_idx_org ON TABLE mig_doc FIELDS org;
DEFINE INDEX mig_idx_email ON TABLE mig_doc FIELDS email UNIQUE;
CREATE mig_doc:1 SET email = 'a@example.com', org = 'surrealdb';
CREATE mig_doc:2 SET email = 'b@example.com', org = 'surrealdb';
CREATE mig_doc:3 SET email = 'c@example.com', org = 'other';
