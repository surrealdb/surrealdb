/**
[test]
reason = "Setup for migration_versioned.surql: a versioned RocksDB store holding three records written together, a timestamp taken between two pauses, and then one record updated and one deleted."
run = false
*/
CREATE vh:kept SET v = 1;
CREATE vh:changed SET v = 1;
CREATE vh:deleted SET v = 1;
SLEEP 50ms;
CREATE vh_marker:before SET at = time::now();
SLEEP 50ms;
UPDATE vh:changed SET v = 2;
DELETE vh:deleted;
