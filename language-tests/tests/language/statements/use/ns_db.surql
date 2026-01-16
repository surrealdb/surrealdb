/**
[test]

[[test.results]]
value = "['test', 'test','test','test']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'test', namespace: 'my_ns' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'my_db', namespace: 'my_ns' }"

[[test.results]]
value = "['my_ns', 'my_ns','my_db','my_db']"

*/
SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
DEFINE NS my_ns;
USE NS my_ns;
DEFINE DB my_db;
USE NS my_ns DB my_db;
SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
