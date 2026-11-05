/**
[env]
remote-exclusive = true
clean = true

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'test', namespace: 'NS' }"

[[test.results]]
value = "{ accesses: {  }, buckets: {  }, databases: {  }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, buckets: {  }, databases: {  }, users: {  } }"

*/

BEGIN;
DEFINE NAMESPACE ns;
USE NS NS;

INFO FOR NS;

COMMIT;

INFO FOR NS;
