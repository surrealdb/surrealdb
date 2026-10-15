/**
[env]
timeout = 30000
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An event whose `WHEN` can never be truthy for an action is never fired by a
write of that action, and every other event is: `false`, `NONE`, `NULL`, `!true`,
comparisons of `$event` with a literal that is false for the action, an `AND` with
a false operand and a `NOT IN` that excludes every action are never truthy, while
`true` and the comparisons that hold for an action fire for it. Every write form
over a table and over an edge table writes one audit row for each event that fires,
and the rows are counted by event, action and table.
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
value = "[{ id: src:1, n: 1 }]"

[[test.results]]
value = "[{ id: src:1, n: 2 }]"

[[test.results]]
value = "[{ id: src:2, n: 1 }]"

[[test.results]]
value = "[{ id: src:2, n: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: src:3, n: 1 }]"

[[test.results]]
value = "[{ id: src:3, n: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: edge:1, in: src:2, n: 1, out: src:3 }]"

[[test.results]]
value = "[{ id: edge:1, in: src:2, n: 2, out: src:3 }]"

[[test.results]]
value = "[{ id: src:2, n: 2, touched: true }, { id: src:3, n: 3, touched: true }, { id: edge:1, in: src:2, n: 2, out: src:3, touched: true }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ act: 'DELETE', count: 1, tag: 'f05_delete', tb: 'edge' }, { act: 'DELETE', count: 3, tag: 'f05_delete', tb: 'src' }, { act: 'CREATE', count: 1, tag: 'f06_create_exact', tb: 'edge' }, { act: 'CREATE', count: 3, tag: 'f06_create_exact', tb: 'src' }, { act: 'DELETE', count: 1, tag: 'f07_in_delete', tb: 'edge' }, { act: 'DELETE', count: 3, tag: 'f07_in_delete', tb: 'src' }, { act: 'CREATE', count: 1, tag: 'l01_true', tb: 'edge' }, { act: 'CREATE', count: 3, tag: 'l01_true', tb: 'src' }, { act: 'DELETE', count: 1, tag: 'l01_true', tb: 'edge' }, { act: 'DELETE', count: 3, tag: 'l01_true', tb: 'src' }, { act: 'UPDATE', count: 2, tag: 'l01_true', tb: 'edge' }, { act: 'UPDATE', count: 5, tag: 'l01_true', tb: 'src' }, { act: 'UPDATE', count: 2, tag: 'l02_update', tb: 'edge' }, { act: 'UPDATE', count: 5, tag: 'l02_update', tb: 'src' }, { act: 'CREATE', count: 1, tag: 'l03_in_create_delete', tb: 'edge' }, { act: 'CREATE', count: 3, tag: 'l03_in_create_delete', tb: 'src' }, { act: 'DELETE', count: 1, tag: 'l03_in_create_delete', tb: 'edge' }, { act: 'DELETE', count: 3, tag: 'l03_in_create_delete', tb: 'src' }, { act: 'UPDATE', count: 2, tag: 'l04_or_false', tb: 'edge' }, { act: 'UPDATE', count: 5, tag: 'l04_or_false', tb: 'src' }, { act: 'CREATE', count: 1, tag: 'l05_and_true', tb: 'edge' }, { act: 'CREATE', count: 3, tag: 'l05_and_true', tb: 'src' }]"
*/

DEFINE TABLE src SCHEMALESS;
DEFINE TABLE edge SCHEMALESS TYPE RELATION;
DEFINE TABLE audit SCHEMALESS;
DEFINE EVENT f01_false ON src WHEN false THEN (CREATE audit SET tag = 'f01_false', act = $event, tb = 'src');
DEFINE EVENT f02_none ON src WHEN NONE THEN (CREATE audit SET tag = 'f02_none', act = $event, tb = 'src');
DEFINE EVENT f03_null ON src WHEN NULL THEN (CREATE audit SET tag = 'f03_null', act = $event, tb = 'src');
DEFINE EVENT f04_not_true ON src WHEN !true THEN (CREATE audit SET tag = 'f04_not_true', act = $event, tb = 'src');
DEFINE EVENT f05_delete ON src WHEN $event = "DELETE" THEN (CREATE audit SET tag = 'f05_delete', act = $event, tb = 'src');
DEFINE EVENT f06_create_exact ON src WHEN $event == "CREATE" THEN (CREATE audit SET tag = 'f06_create_exact', act = $event, tb = 'src');
DEFINE EVENT f07_in_delete ON src WHEN $event IN ["DELETE"] THEN (CREATE audit SET tag = 'f07_in_delete', act = $event, tb = 'src');
DEFINE EVENT f08_self_ne ON src WHEN $event != $event THEN (CREATE audit SET tag = 'f08_self_ne', act = $event, tb = 'src');
DEFINE EVENT f09_and_false ON src WHEN $event = "UPDATE" AND false THEN (CREATE audit SET tag = 'f09_and_false', act = $event, tb = 'src');
DEFINE EVENT f10_not_in ON src WHEN $event NOT IN ["CREATE", "UPDATE", "DELETE"] THEN (CREATE audit SET tag = 'f10_not_in', act = $event, tb = 'src');
DEFINE EVENT f11_contains ON src WHEN ["CREATE"] CONTAINS "UPDATE" THEN (CREATE audit SET tag = 'f11_contains', act = $event, tb = 'src');
DEFINE EVENT l01_true ON src WHEN true THEN (CREATE audit SET tag = 'l01_true', act = $event, tb = 'src');
DEFINE EVENT l02_update ON src WHEN $event = "UPDATE" THEN (CREATE audit SET tag = 'l02_update', act = $event, tb = 'src');
DEFINE EVENT l03_in_create_delete ON src WHEN $event IN ["CREATE", "DELETE"] THEN (CREATE audit SET tag = 'l03_in_create_delete', act = $event, tb = 'src');
DEFINE EVENT l04_or_false ON src WHEN $event = "UPDATE" OR false THEN (CREATE audit SET tag = 'l04_or_false', act = $event, tb = 'src');
DEFINE EVENT l05_and_true ON src WHEN $event = "CREATE" AND true THEN (CREATE audit SET tag = 'l05_and_true', act = $event, tb = 'src');
DEFINE EVENT f01_false ON edge WHEN false THEN (CREATE audit SET tag = 'f01_false', act = $event, tb = 'edge');
DEFINE EVENT f02_none ON edge WHEN NONE THEN (CREATE audit SET tag = 'f02_none', act = $event, tb = 'edge');
DEFINE EVENT f03_null ON edge WHEN NULL THEN (CREATE audit SET tag = 'f03_null', act = $event, tb = 'edge');
DEFINE EVENT f04_not_true ON edge WHEN !true THEN (CREATE audit SET tag = 'f04_not_true', act = $event, tb = 'edge');
DEFINE EVENT f05_delete ON edge WHEN $event = "DELETE" THEN (CREATE audit SET tag = 'f05_delete', act = $event, tb = 'edge');
DEFINE EVENT f06_create_exact ON edge WHEN $event == "CREATE" THEN (CREATE audit SET tag = 'f06_create_exact', act = $event, tb = 'edge');
DEFINE EVENT f07_in_delete ON edge WHEN $event IN ["DELETE"] THEN (CREATE audit SET tag = 'f07_in_delete', act = $event, tb = 'edge');
DEFINE EVENT f08_self_ne ON edge WHEN $event != $event THEN (CREATE audit SET tag = 'f08_self_ne', act = $event, tb = 'edge');
DEFINE EVENT f09_and_false ON edge WHEN $event = "UPDATE" AND false THEN (CREATE audit SET tag = 'f09_and_false', act = $event, tb = 'edge');
DEFINE EVENT f10_not_in ON edge WHEN $event NOT IN ["CREATE", "UPDATE", "DELETE"] THEN (CREATE audit SET tag = 'f10_not_in', act = $event, tb = 'edge');
DEFINE EVENT f11_contains ON edge WHEN ["CREATE"] CONTAINS "UPDATE" THEN (CREATE audit SET tag = 'f11_contains', act = $event, tb = 'edge');
DEFINE EVENT l01_true ON edge WHEN true THEN (CREATE audit SET tag = 'l01_true', act = $event, tb = 'edge');
DEFINE EVENT l02_update ON edge WHEN $event = "UPDATE" THEN (CREATE audit SET tag = 'l02_update', act = $event, tb = 'edge');
DEFINE EVENT l03_in_create_delete ON edge WHEN $event IN ["CREATE", "DELETE"] THEN (CREATE audit SET tag = 'l03_in_create_delete', act = $event, tb = 'edge');
DEFINE EVENT l04_or_false ON edge WHEN $event = "UPDATE" OR false THEN (CREATE audit SET tag = 'l04_or_false', act = $event, tb = 'edge');
DEFINE EVENT l05_and_true ON edge WHEN $event = "CREATE" AND true THEN (CREATE audit SET tag = 'l05_and_true', act = $event, tb = 'edge');
CREATE src:1 SET n = 1;
UPDATE src:1 SET n = 2;
UPSERT src:2 SET n = 1;
UPSERT src:2 SET n = 2;
DELETE src:1;
INSERT INTO src { id: src:3, n: 1 };
INSERT INTO src { id: src:3, n: 2 } ON DUPLICATE KEY UPDATE n = 3;
INSERT IGNORE INTO src { id: src:3, n: 4 };
RELATE src:2->edge:1->src:3 SET n = 1;
RELATE src:2->edge:1->src:3 SET n = 2;
UPDATE src, edge SET touched = true;
DELETE edge:1;
DELETE src;
SELECT tag, act, tb, count() FROM audit GROUP BY tag, act, tb ORDER BY tag, act, tb;
