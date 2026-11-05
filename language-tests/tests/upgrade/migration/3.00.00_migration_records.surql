/**
[env]
imports = ["upgrade/migration/3.00.00_migration_records_import.surql"]

[test]
upgrade = true
migration = true
reason = "Records, an update and a delete written to a RocksDB store are what SurrealKV reads after migrating that directory in place."

[[test.results]]
value = "[{ age: 30, id: mig_person:1, name: 'alice' }, { age: 42, id: mig_person:2, name: 'bob' }]"

[[test.results]]
value = "[{ count: 2 }]"

*/
SELECT * FROM mig_person ORDER BY id;
SELECT count() FROM mig_person GROUP ALL;
