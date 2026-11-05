/**
[test]
reason = "Setup for migration_graph.surql: a RocksDB store holding a vertex with outbound and inbound edges."
run = false
*/
DEFINE TABLE mig_user SCHEMALESS;
DEFINE TABLE mig_follows TYPE RELATION FROM mig_user TO mig_user SCHEMALESS;
CREATE mig_user:hub;
CREATE mig_user:a;
CREATE mig_user:b;
CREATE mig_user:c;
RELATE mig_user:hub -> mig_follows:o1 -> mig_user:a SET weight = 1;
RELATE mig_user:hub -> mig_follows:o2 -> mig_user:b SET weight = 2;
RELATE mig_user:c -> mig_follows:i1 -> mig_user:hub SET weight = 3;
