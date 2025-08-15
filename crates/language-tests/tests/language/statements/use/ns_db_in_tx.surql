/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, databases: {  }, users: {  } }"

[[test.results]]
value = "{ accesses: {  }, databases: {  }, users: {  } }"

*/

BEGIN;
DEFINE NAMESPACE ns;
USE NS NS;

INFO FOR NS;

COMMIT;

INFO FOR NS;