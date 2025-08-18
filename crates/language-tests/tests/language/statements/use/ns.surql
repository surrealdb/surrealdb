/**
[test]

[[test.results]]
value = "['test', 'test', 'test', 'test']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['my_ns', 'my_ns', 'test', 'test']"

*/

SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
USE NS my_ns DB test;
SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
