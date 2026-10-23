/**
[env]
imports = ["upgrade/migration/3.00.00_migration_graph_import.surql"]

[test]
upgrade = true
migration = true
reason = "Graph adjacency written to RocksDB still resolves in both directions after the directory is migrated to SurrealKV."

[[test.results]]
value = "[mig_user:a, mig_user:b]"

[[test.results]]
value = "[mig_user:c]"

[[test.results]]
value = "[mig_follows:o2]"

*/
SELECT VALUE array::sort(->mig_follows->mig_user) FROM ONLY mig_user:hub;
SELECT VALUE <-mig_follows<-mig_user FROM ONLY mig_user:hub;
SELECT VALUE ->(mig_follows WHERE weight = 2) FROM ONLY mig_user:hub;
