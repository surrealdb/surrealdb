/**
[env]
compatibility = 3
imports = ["upgrade/define/3.00.00_layout_generations_import.surql"]

[test]
upgrade = true
reason = "Schema changes on the current build over a SCHEMAFULL table a released binary wrote mint the table's layout generations, and rows of every generation, and the old `revision` rows of none, read as their schema says: an added field absent on old rows, a removed one still held by the rows that stored it. Removing the table and defining it again starts it from nothing: the rows and every generation are gone."
# Layout generations are the current checkout's; older targets have none.
version = ">=3.4.0-a"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 41, id: person:1, legacy: 'kept until rewritten', name: 'Ann' }, { age: 37, id: person:2, name: 'Bob' }, { age: 29, email: 'cy@example.com', id: person:3, name: 'Cy', nick: 'c' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 41, id: person:1, legacy: 'kept until rewritten', name: 'Ann' }, { age: 38, id: person:2, name: 'Bob' }, { age: 29, email: 'cy@example.com', id: person:3, name: 'Cy', nick: 'c' }, { age: 52, id: person:4, name: 'Di' }]"

[[test.results]]
value = "[{ email: NONE, name: 'Ann' }, { email: NONE, name: 'Bob' }, { email: NONE, name: 'Di' }]"

[[test.results]]
value = "[{ c: 4, oldest: 52 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: person:1, name: 'Zed' }]"

*/
-- Adding fields; old rows read them as absent.
DEFINE FIELD email ON person TYPE option<string>;
DEFINE FIELD nick ON person TYPE option<string>;
CREATE person:3 SET name = 'Cy', age = 29, email = 'cy@example.com', nick = 'c' RETURN NONE;
SELECT * FROM person ORDER BY id;
-- Removing a field; rows keep what they stored.
REMOVE FIELD nick ON person;
CREATE person:4 SET name = 'Di', age = 52 RETURN NONE;
UPDATE person:2 SET age = 38 RETURN NONE;
SELECT * FROM person ORDER BY id;
-- A filter and a projection over rows of every generation.
SELECT name, email FROM person WHERE age > 30 ORDER BY name;
SELECT count() AS c, math::max(age) AS oldest FROM person GROUP ALL;
-- Removing the table clears its rows and generations; a table defined again under the name starts empty.
REMOVE TABLE person;
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
CREATE person:1 SET name = 'Zed' RETURN NONE;
SELECT * FROM person;
