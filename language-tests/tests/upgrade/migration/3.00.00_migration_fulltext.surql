/**
[env]
imports = ["upgrade/migration/3.00.00_migration_fulltext_import.surql"]

[test]
upgrade = true
migration = true
reason = "A full-text index written to RocksDB still matches the same documents after the directory is migrated to SurrealKV."

[[test.results]]
value = "[{ id: mig_blog:1 }, { id: mig_blog:2 }, { id: mig_blog:4 }]"

[[test.results]]
value = "[{ id: mig_blog:3 }, { id: mig_blog:4 }]"

*/
SELECT id FROM mig_blog WHERE title @1@ 'dog' ORDER BY id;
SELECT id FROM mig_blog WHERE title @1@ 'sat' ORDER BY id;
