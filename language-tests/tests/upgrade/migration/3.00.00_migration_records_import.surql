/**
[test]
reason = "Setup for migration_records.surql: a RocksDB store holding plain records."
run = false
*/
CREATE mig_person:1 SET name = 'alice', age = 30;
CREATE mig_person:2 SET name = 'bob', age = 41;
CREATE mig_person:3 SET name = 'carol', age = 25;
UPDATE mig_person:2 SET age = 42;
DELETE mig_person:3;
