/**
[env]
timeout = 30000
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An event whose `WHEN` is never truthy for the action, and the same event with a `WHEN` that
is no longer decidable ahead of time (an `AND` with a call that draws), leave the same audit
rows, the same records and the same errors after the same writes of every form. The pair of
tables `da` and `db` writes an audit row for each event that fires; the pair `dc` and `dd`
holds an event that raises for `DELETE` and none of the other actions.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: da:1, n: 1 }, { id: da:2, n: 1 }, { id: da:3, n: 1 }, { id: da:4, n: 1 }, { id: da:5, n: 1 }]"

[[test.results]]
value = "[{ id: db:1, n: 1 }, { id: db:2, n: 1 }, { id: db:3, n: 1 }, { id: db:4, n: 1 }, { id: db:5, n: 1 }]"

[[test.results]]
value = "[{ id: dc:1, n: 1 }, { id: dc:2, n: 1 }, { id: dc:3, n: 1 }, { id: dc:4, n: 1 }, { id: dc:5, n: 1 }]"

[[test.results]]
value = "[{ id: dd:1, n: 1 }, { id: dd:2, n: 1 }, { id: dd:3, n: 1 }, { id: dd:4, n: 1 }, { id: dd:5, n: 1 }]"

[[test.results]]
value = "[{ id: da:1, n: 2 }, { id: da:2, n: 2 }, { id: da:3, n: 2 }, { id: da:4, n: 2 }, { id: da:5, n: 2 }]"

[[test.results]]
value = "[{ id: db:1, n: 2 }, { id: db:2, n: 2 }, { id: db:3, n: 2 }, { id: db:4, n: 2 }, { id: db:5, n: 2 }]"

[[test.results]]
value = "[{ id: dc:1, n: 2 }, { id: dc:2, n: 2 }, { id: dc:3, n: 2 }, { id: dc:4, n: 2 }, { id: dc:5, n: 2 }]"

[[test.results]]
value = "[{ id: dd:1, n: 2 }, { id: dd:2, n: 2 }, { id: dd:3, n: 2 }, { id: dd:4, n: 2 }, { id: dd:5, n: 2 }]"

[[test.results]]
value = "[{ id: da:6, n: 1 }]"

[[test.results]]
value = "[{ id: db:6, n: 1 }]"

[[test.results]]
value = "[{ id: da:6, n: 2 }]"

[[test.results]]
value = "[{ id: db:6, n: 2 }]"

[[test.results]]
value = "[{ id: da:7, n: 1 }]"

[[test.results]]
value = "[{ id: db:7, n: 1 }]"

[[test.results]]
value = "[{ id: da:7, n: 3 }]"

[[test.results]]
value = "[{ id: db:7, n: 3 }]"

[[test.results]]
value = "[{ act: 'CREATE', count: 7, tag: 'e_live' }, { act: 'UPDATE', count: 7, tag: 'e_live' }, { act: 'UPDATE', count: 7, tag: 'e_update' }]"

[[test.results]]
value = "[{ act: 'CREATE', count: 7, tag: 'e_live' }, { act: 'UPDATE', count: 7, tag: 'e_live' }, { act: 'UPDATE', count: 7, tag: 'e_update' }]"

[[test.results]]
value = "[{ act: 'CREATE', count: 5, tag: 'e_live' }, { act: 'UPDATE', count: 5, tag: 'e_live' }]"

[[test.results]]
value = "[{ act: 'CREATE', count: 5, tag: 'e_live' }, { act: 'UPDATE', count: 5, tag: 'e_live' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Error while processing event e_boom: An error occurred: boom"

[[test.results]]
error = "Error while processing event e_boom: An error occurred: boom"

[[test.results]]
value = "[{ id: da:7, n: 3 }]"

[[test.results]]
value = "[{ id: db:7, n: 3 }]"

[[test.results]]
value = "[{ id: da:1, n: 2 }, { id: da:2, n: 2 }, { id: da:3, n: 2 }, { id: da:4, n: 2 }, { id: da:5, n: 2 }, { id: da:6, n: 2 }]"

[[test.results]]
value = "[{ id: db:1, n: 2 }, { id: db:2, n: 2 }, { id: db:3, n: 2 }, { id: db:4, n: 2 }, { id: db:5, n: 2 }, { id: db:6, n: 2 }]"

[[test.results]]
value = "[{ id: dc:1, n: 2 }, { id: dc:2, n: 2 }, { id: dc:3, n: 2 }, { id: dc:4, n: 2 }, { id: dc:5, n: 2 }]"

[[test.results]]
value = "[{ id: dd:1, n: 2 }, { id: dd:2, n: 2 }, { id: dd:3, n: 2 }, { id: dd:4, n: 2 }, { id: dd:5, n: 2 }]"

[[test.results]]
value = "[{ act: 'DELETE', count: 1, tag: 'e_delete' }, { act: 'DELETE', count: 1, tag: 'e_in_delete' }, { act: 'CREATE', count: 7, tag: 'e_live' }, { act: 'DELETE', count: 1, tag: 'e_live' }, { act: 'UPDATE', count: 7, tag: 'e_live' }, { act: 'UPDATE', count: 7, tag: 'e_update' }]"

[[test.results]]
value = "[{ act: 'DELETE', count: 1, tag: 'e_delete' }, { act: 'DELETE', count: 1, tag: 'e_in_delete' }, { act: 'CREATE', count: 7, tag: 'e_live' }, { act: 'DELETE', count: 1, tag: 'e_live' }, { act: 'UPDATE', count: 7, tag: 'e_live' }, { act: 'UPDATE', count: 7, tag: 'e_update' }]"
*/

DEFINE TABLE audit SCHEMALESS;
DEFINE TABLE da SCHEMALESS;
DEFINE TABLE db SCHEMALESS;
DEFINE TABLE dc SCHEMALESS;
DEFINE TABLE dd SCHEMALESS;
DEFINE EVENT e_delete ON da WHEN $event = "DELETE" THEN (CREATE audit SET tbl = 'da', tag = 'e_delete', act = $event, rec = $value.id);
DEFINE EVENT e_create_false ON da WHEN $event = "CREATE" AND false THEN (CREATE audit SET tbl = 'da', tag = 'e_create_false', act = $event, rec = $value.id);
DEFINE EVENT e_in_delete ON da WHEN $event IN ["DELETE"] THEN (CREATE audit SET tbl = 'da', tag = 'e_in_delete', act = $event, rec = $value.id);
DEFINE EVENT e_live ON da WHEN true THEN (CREATE audit SET tbl = 'da', tag = 'e_live', act = $event, rec = $value.id);
DEFINE EVENT e_update ON da WHEN $event = "UPDATE" THEN (CREATE audit SET tbl = 'da', tag = 'e_update', act = $event, rec = $value.id);
DEFINE EVENT e_delete ON db WHEN $event = "DELETE" AND rand() >= 0 THEN (CREATE audit SET tbl = 'db', tag = 'e_delete', act = $event, rec = $value.id);
DEFINE EVENT e_create_false ON db WHEN $event = "CREATE" AND false AND rand() >= 0 THEN (CREATE audit SET tbl = 'db', tag = 'e_create_false', act = $event, rec = $value.id);
DEFINE EVENT e_in_delete ON db WHEN $event IN ["DELETE"] AND rand() >= 0 THEN (CREATE audit SET tbl = 'db', tag = 'e_in_delete', act = $event, rec = $value.id);
DEFINE EVENT e_live ON db WHEN true AND rand() >= 0 THEN (CREATE audit SET tbl = 'db', tag = 'e_live', act = $event, rec = $value.id);
DEFINE EVENT e_update ON db WHEN $event = "UPDATE" AND rand() >= 0 THEN (CREATE audit SET tbl = 'db', tag = 'e_update', act = $event, rec = $value.id);
DEFINE EVENT e_boom ON dc WHEN $event = "DELETE" THEN (THROW 'boom');
DEFINE EVENT e_live ON dc WHEN true THEN (CREATE audit SET tbl = 'dc', tag = 'e_live', act = $event, rec = $value.id);
DEFINE EVENT e_boom ON dd WHEN $event = "DELETE" AND rand() >= 0 THEN (THROW 'boom');
DEFINE EVENT e_live ON dd WHEN true THEN (CREATE audit SET tbl = 'dd', tag = 'e_live', act = $event, rec = $value.id);
CREATE |da:1..=5| SET n = 1;
CREATE |db:1..=5| SET n = 1;
CREATE |dc:1..=5| SET n = 1;
CREATE |dd:1..=5| SET n = 1;
UPDATE da SET n = n + 1;
UPDATE db SET n = n + 1;
UPDATE dc SET n = n + 1;
UPDATE dd SET n = n + 1;
UPSERT da:6 SET n = 1;
UPSERT db:6 SET n = 1;
UPSERT da:6 SET n = 2;
UPSERT db:6 SET n = 2;
INSERT INTO da { id: da:7, n: 1 };
INSERT INTO db { id: db:7, n: 1 };
INSERT INTO da { id: da:7, n: 2 } ON DUPLICATE KEY UPDATE n = 3;
INSERT INTO db { id: db:7, n: 2 } ON DUPLICATE KEY UPDATE n = 3;
SELECT tag, act, count() FROM audit WHERE tbl = 'da' GROUP BY tag, act ORDER BY tag, act;
SELECT tag, act, count() FROM audit WHERE tbl = 'db' GROUP BY tag, act ORDER BY tag, act;
SELECT tag, act, count() FROM audit WHERE tbl = 'dc' GROUP BY tag, act ORDER BY tag, act;
SELECT tag, act, count() FROM audit WHERE tbl = 'dd' GROUP BY tag, act ORDER BY tag, act;
DELETE dc WHERE n > 100;
DELETE dd WHERE n > 100;
DELETE dc:1;
DELETE dd:1;
DELETE da WHERE n > 2 RETURN BEFORE;
DELETE db WHERE n > 2 RETURN BEFORE;
SELECT * FROM da ORDER BY id;
SELECT * FROM db ORDER BY id;
SELECT * FROM dc ORDER BY id;
SELECT * FROM dd ORDER BY id;
SELECT tag, act, count() FROM audit WHERE tbl = 'da' GROUP BY tag, act ORDER BY tag, act;
SELECT tag, act, count() FROM audit WHERE tbl = 'db' GROUP BY tag, act ORDER BY tag, act;
