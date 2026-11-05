/**
[env]
namespace = true
database = true
imports = ["pgsql/_id_key_empty.surql"]

[test]
reason = "An expression that is compared with the id of a PostgreSQL table, in a WHERE clause, an IN list and a join key, names a record by its value, as a column does. An expression that is NULL, absent, empty or a float names no record, and the absent id that an outer join leaves on its null-extended side is not matched by an expression that is absent."

[[test.results]]
value = "[{ name: 'one' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ name: 'one' }, { name: 'two' }]"

[[test.results]]
value = "[{ label: 'plain key', name: 'one' }, { label: 'record link', name: 'two' }]"

[[test.results]]
value = "[{ label: 'bool key', name: NONE }, { label: 'empty key', name: NONE }, { label: 'float key', name: NONE }, { label: 'no key', name: NONE }, { label: 'null key', name: NONE }, { label: 'plain key', name: 'one' }, { label: 'record link', name: 'two' }]"

[[test.results]]
value = "[{ label: 'bool key', name: NONE, other: NONE }, { label: 'empty key', name: NONE, other: NONE }, { label: 'float key', name: NONE, other: NONE }, { label: 'no key', name: NONE, other: NONE }, { label: 'null key', name: NONE, other: NONE }, { label: 'plain key', name: 'one', other: NONE }, { label: 'record link', name: 'two', other: NONE }]"

[[test.results]]
value = "[{ label: 'bool key', name: NONE, other: NONE }, { label: 'empty key', name: NONE, other: NONE }, { label: 'float key', name: NONE, other: NONE }, { label: 'no key', name: NONE, other: NONE }, { label: 'null key', name: NONE, other: NONE }, { label: 'plain key', name: 'one', other: NONE }, { label: 'record link', name: 'two', other: NONE }]"
*/

SELECT name FROM owner WHERE id = CASE WHEN name = 'one' THEN 1 WHEN name = 'two' THEN '' END ORDER BY name;

SELECT name FROM owner WHERE id = CASE WHEN name = name THEN 1.5 END ORDER BY name;

SELECT name FROM owner WHERE id IN (CASE WHEN name = 'one' THEN 1 END, CASE WHEN name = 'two' THEN 2 ELSE NULL END) ORDER BY name;

SELECT p.label, o.name FROM pet p JOIN owner o ON CASE WHEN p.label = 'no key' THEN NULL ELSE p.owner END = o.id ORDER BY p.label;

SELECT p.label, o.name FROM pet p LEFT JOIN owner o ON CASE WHEN p.label = 'no key' THEN NULL ELSE p.owner END = o.id ORDER BY p.label;

SELECT p.label, o.name, q.label AS other FROM pet p LEFT JOIN owner o ON p.owner = o.id LEFT JOIN pet q ON o.id = CASE WHEN q.label = 'none such' THEN 1 END ORDER BY p.label;

SELECT p.label, o.name, q.label AS other FROM pet p LEFT JOIN owner o ON p.owner = o.id LEFT JOIN pet q ON CASE WHEN q.label = 'none such' THEN 1 END = o.id ORDER BY p.label;
