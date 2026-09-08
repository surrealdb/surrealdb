/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE TABLE log_src;
DEFINE TABLE evt_log SCHEMALESS;
DEFINE EVENT sync_evt ON log_src WHEN true THEN (CREATE evt_log SET k = 'sync');
DEFINE EVENT async_evt ON log_src ASYNC RETRY 2 MAXDEPTH 3 WHEN true THEN (CREATE evt_log SET k = 'async');
