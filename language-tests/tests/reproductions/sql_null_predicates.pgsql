/**
[env]
namespace = true
database = true
imports = ["reproductions/sql_null_predicates_import.surql"]

[test]
reason = "SQL predicates exclude unknown from writes, preserve unknown under NOT and IN, and test missing fields as NULL."

[[test.results]]
value = "[{ id: 3 }, { id: 5 }]"
[[test.results]]
value = "[{ id: 3 }, { id: 5 }]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[{ id: 1 }, { id: 4 }]"
[[test.results]]
value = "[{ id: 5 }]"
[[test.results]]
value = "[{ id: 3 }, { id: 5 }]"
[[test.results]]
value = "[{ id: 1 }, { id: 2 }, { id: 4 }]"
*/
SELECT id FROM null_pred WHERE v <> 1 ORDER BY id;
UPDATE null_pred SET touched = true WHERE v <> 1 RETURNING id;
DELETE FROM null_pred WHERE v NOT IN (1, NULL) RETURNING id;
SELECT id FROM null_pred WHERE v IS NULL ORDER BY id;
SELECT id FROM null_pred WHERE v NOT BETWEEN 1 AND 2 ORDER BY id;
DELETE FROM null_pred WHERE NOT (v = 1) RETURNING id;
SELECT id FROM null_pred ORDER BY id;
