/**
[test]

[[test.results]]
value = "['test', 'test', 'test', 'test']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['my_ns', 'my_ns', 'test', 'test']"

*/

SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
DEFINE NS my_ns;
USE NS my_ns;
DEFINE DB test;
SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
