/**
[env]
namespace = true
database = true
imports = ["pgsql/_id_key_empty.surql"]

[test]
reason = "A foreign key that is the empty string, NULL, absent, a float or a boolean names no record: it matches nothing and does not stop the statement, while a plain key and a record link beside it still match"

[[test.results]]
value = "[{ label: 'plain key', name: 'one' }, { label: 'record link', name: 'two' }]"

[[test.results]]
value = "[{ label: 'bool key', name: NONE }, { label: 'empty key', name: NONE }, { label: 'float key', name: NONE }, { label: 'no key', name: NONE }, { label: 'null key', name: NONE }, { label: 'plain key', name: 'one' }, { label: 'record link', name: 'two' }]"

[[test.results]]
value = "[{ label: 'empty key' }]"

[[test.results]]
value = "[{ name: 'one' }]"
*/

SELECT p.label, o.name FROM pet p JOIN owner o ON p.owner = o.id ORDER BY p.label;

SELECT p.label, o.name FROM pet p LEFT JOIN owner o ON p.owner = o.id ORDER BY p.label;

SELECT label FROM pet WHERE owner = '' ORDER BY label;

SELECT name FROM owner WHERE id IN (SELECT 1) ORDER BY name;
