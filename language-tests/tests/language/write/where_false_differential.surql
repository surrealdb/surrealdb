/**
[env]
timeout = 30000
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A condition that is the literal `false` and a condition that is never truthy but is not a
literal (`rand() < 0`) answer alike for every statement of `where_false_named_targets`:
the same result or the same error, and the same records, indexes, events and views
afterwards. The second statement of each pair keeps every stage of its order.
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
value = "[{ id: person:1 }, { id: person:2 }]"

[[test.results]]
value = "[{ id: knows:[person:1, person:2], in: person:1, out: person:2 }]"

[[test.results]]
value = "[{ id: t:1, x: 0 }]"

[[test.results]]
value = "[{ id: ti:1, n: 2 }]"

[[test.results]]
value = "[{ id: ti:2, n: 3 }]"

[[test.results]]
value = "[{ id: te:1, n: 1 }]"

[[test.results]]
value = "[{ id: te:2, n: 2 }]"

[[test.results]]
error = "Cannot write to the `tv` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
error = "Cannot write to the `tv` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be updated: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be updated: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be updated: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be updated: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1, x: 0 }]"

[[test.results]]
value = "[{ id: ti:1, n: 2 }, { id: ti:2, n: 3 }]"

[[test.results]]
value = "[{ id: te:1, n: 1 }, { id: te:2, n: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ c: 2, id: tv:[] }]"

[[test.results]]
value = "[{ id: knows:[person:1, person:2], in: person:1, out: person:2 }]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE TABLE person SCHEMALESS;
DEFINE TABLE knows TYPE RELATION IN person OUT person LIGHTWEIGHT;
-- A schema, an index and a VALUE clause: nothing after the WHERE stage needs a unit.
DEFINE TABLE ti SCHEMAFULL;
DEFINE FIELD n ON ti TYPE int VALUE $value + 1;
DEFINE INDEX ti_n ON ti FIELDS n UNIQUE;
-- An event, an index, a field and a view over the table.
DEFINE TABLE audit SCHEMALESS;
DEFINE TABLE te SCHEMALESS;
DEFINE FIELD n ON te TYPE int;
DEFINE INDEX te_n ON te FIELDS n;
DEFINE EVENT ev ON te THEN (CREATE audit SET src = $value.id, act = $event);
DEFINE TABLE tv AS SELECT count() AS c FROM te GROUP ALL;
CREATE person:1, person:2;
RELATE person:1->knows->person:2;
CREATE t:1 SET x = 0;
CREATE ti:1 SET n = 1;
CREATE ti:2 SET n = 2;
CREATE te:1 SET n = 1;
CREATE te:2 SET n = 2;
-- Each statement runs with the literal `false` and with a condition that is never truthy but is no literal.
UPDATE tv:[] SET x = 1 WHERE false;
UPDATE tv:[] SET x = 1 WHERE rand() < 0;
UPDATE knows:[person:1, person:2] WHERE false;
UPDATE knows:[person:1, person:2] WHERE rand() < 0;
UPDATE knows:[person:1, person:2] SET x = 1 WHERE false;
UPDATE knows:[person:1, person:2] SET x = 1 WHERE rand() < 0;
DELETE knows:[person:1, person:2] WHERE false;
DELETE knows:[person:1, person:2] WHERE rand() < 0;
DELETE t:missing WHERE false;
DELETE t:missing WHERE rand() < 0;
UPDATE t:missing SET x = 1 WHERE false;
UPDATE t:missing SET x = 1 WHERE rand() < 0;
UPDATE t:1, t:2 SET x = 1 WHERE false RETURN BEFORE;
UPDATE t:1, t:2 SET x = 1 WHERE rand() < 0 RETURN BEFORE;
UPDATE t:1, t:2 SET x = 1 WHERE false RETURN AFTER;
UPDATE t:1, t:2 SET x = 1 WHERE rand() < 0 RETURN AFTER;
UPDATE t:1, t:2 SET x = 1 WHERE false RETURN DIFF;
UPDATE t:1, t:2 SET x = 1 WHERE rand() < 0 RETURN DIFF;
UPDATE t:1, t:2 SET x = 1 WHERE false RETURN NONE;
UPDATE t:1, t:2 SET x = 1 WHERE rand() < 0 RETURN NONE;
UPDATE t:1, t:2 SET x = 1 WHERE false RETURN NULL;
UPDATE t:1, t:2 SET x = 1 WHERE rand() < 0 RETURN NULL;
UPDATE t:1, t:2 SET x = 1 WHERE false RETURN id;
UPDATE t:1, t:2 SET x = 1 WHERE rand() < 0 RETURN id;
UPDATE t:1, t:2 SET x = 1 WHERE false;
UPDATE t:1, t:2 SET x = 1 WHERE rand() < 0;
UPDATE ONLY t:1 SET x = 1 WHERE false;
UPDATE ONLY t:1 SET x = 1 WHERE rand() < 0;
UPDATE ONLY t:1 SET x = 1 WHERE false RETURN NONE;
UPDATE ONLY t:1 SET x = 1 WHERE rand() < 0 RETURN NONE;
DELETE t:1, t:2 WHERE false RETURN BEFORE;
DELETE t:1, t:2 WHERE rand() < 0 RETURN BEFORE;
DELETE t:1, t:2 WHERE false RETURN NONE;
DELETE t:1, t:2 WHERE rand() < 0 RETURN NONE;
DELETE t:1, t:2 WHERE false;
DELETE t:1, t:2 WHERE rand() < 0;
DELETE ONLY t:1 WHERE false;
DELETE ONLY t:1 WHERE rand() < 0;
UPDATE t SET x = 1 WHERE false;
UPDATE t SET x = 1 WHERE rand() < 0;
DELETE t WHERE false;
DELETE t WHERE rand() < 0;
UPDATE t:1 SET x = <int> 'bad' WHERE false;
UPDATE t:1 SET x = <int> 'bad' WHERE rand() < 0;
UPDATE t:1 SET x = fn::missing() WHERE false;
UPDATE t:1 SET x = fn::missing() WHERE rand() < 0;
UPDATE t:1 SET x = (THROW 'boom') WHERE false;
UPDATE t:1 SET x = (THROW 'boom') WHERE rand() < 0;
UPDATE ti:1, ti:2 SET n = 5 WHERE false;
UPDATE ti:1, ti:2 SET n = 5 WHERE rand() < 0;
UPDATE ti:1, ti:2 SET n = 'bad' WHERE false;
UPDATE ti:1, ti:2 SET n = 'bad' WHERE rand() < 0;
UPDATE ti:1 SET n = 5 WHERE false RETURN AFTER;
UPDATE ti:1 SET n = 5 WHERE rand() < 0 RETURN AFTER;
UPDATE ti SET n = 5 WHERE false;
UPDATE ti SET n = 5 WHERE rand() < 0;
DELETE ti:1, ti:2 WHERE false RETURN BEFORE;
DELETE ti:1, ti:2 WHERE rand() < 0 RETURN BEFORE;
DELETE ti WHERE false;
DELETE ti WHERE rand() < 0;
UPDATE te SET n = 5 WHERE false;
UPDATE te SET n = 5 WHERE rand() < 0;
UPDATE te:1, te:2 SET n = 5 WHERE false;
UPDATE te:1, te:2 SET n = 5 WHERE rand() < 0;
UPDATE te:1, te:2 SET n = 'bad' WHERE false;
UPDATE te:1, te:2 SET n = 'bad' WHERE rand() < 0;
UPDATE te:1 SET n = 5 WHERE false RETURN AFTER;
UPDATE te:1 SET n = 5 WHERE rand() < 0 RETURN AFTER;
DELETE te WHERE false;
DELETE te WHERE rand() < 0;
DELETE te:1, te:2 WHERE false RETURN BEFORE;
DELETE te:1, te:2 WHERE rand() < 0 RETURN BEFORE;
SELECT * FROM t ORDER BY id;
SELECT * FROM ti ORDER BY id;
SELECT * FROM te ORDER BY id;
SELECT count() FROM audit GROUP ALL;
SELECT * FROM tv;
SELECT * FROM knows ORDER BY id;
