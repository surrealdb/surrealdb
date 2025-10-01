/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "['test', 'test', 'test', 'test']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['test', 'test', 'my_db', 'my_db']"

*/
DEFINE DATABASE my_db;
SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
USE DB my_db;
SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
