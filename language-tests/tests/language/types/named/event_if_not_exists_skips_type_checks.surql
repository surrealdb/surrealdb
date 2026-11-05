/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE EVENT IF NOT EXISTS that meets the event stores nothing, so the types its clauses name are not judged; a definition that is stored is judged"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ e: 'DEFINE EVENT e ON t WHEN true THEN (CREATE log SET v = 1)' }"

[[test.results]]
error = "Invalid statement: The event 'g' cannot be defined: the type 'missing' it names does not exist"

*/
DEFINE EVENT e ON t THEN (CREATE log SET v = 1);
DEFINE EVENT IF NOT EXISTS e ON t WHEN <missing>$after.status = 'ok' THEN {};
(INFO FOR TABLE t).events;
DEFINE EVENT IF NOT EXISTS g ON t WHEN <missing>$after.status = 'ok' THEN {};
