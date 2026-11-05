-- A relation table with edges, some written before the upgrade and some behind a migration of an
-- edge field, removed and defined again under the same name: the removed edges are not traversed
-- from the nodes, the new table reads only what it holds, and an edge written under an id the
-- removed table used reads as written with its new ends.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE TABLE knows TYPE RELATION IN person OUT person ENFORCED SCHEMAFULL;
DEFINE FIELD since ON knows TYPE string;
CREATE person:1 SET name = 'a' RETURN NONE;
CREATE person:2 SET name = 'b' RETURN NONE;
CREATE person:3 SET name = 'c' RETURN NONE;
RELATE person:1->knows:e1->person:2 SET since = '2020' RETURN NONE;
RELATE person:2->knows:e2->person:3 SET since = '2021' RETURN NONE;
-- @upgrade
ALTER FIELD since ON knows TYPE int MIGRATE EXISTING ROWS AS since_int USING <int>$value FALLBACK 0;
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
SELECT * FROM knows ORDER BY id;
-- @check
REMOVE TABLE knows;
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
SELECT VALUE <-knows<-person.id FROM person ORDER BY id;
SELECT * FROM knows ORDER BY id;
DEFINE TABLE knows TYPE RELATION IN person OUT person ENFORCED SCHEMAFULL;
DEFINE FIELD since ON knows TYPE string;
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
RELATE person:1->knows:e1->person:3 SET since = 'new' RETURN NONE;
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
SELECT VALUE <-knows<-person.id FROM person ORDER BY id;
SELECT * FROM knows ORDER BY id;
-- @drain
SELECT * FROM knows ORDER BY id;
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
(INFO FOR TABLE knows).migrations;
-- A migration of the removed table's name, on the new table, after the drain.
ALTER FIELD since ON knows TYPE int MIGRATE EXISTING ROWS AS since_int USING <int>$value FALLBACK 0;
SELECT * FROM knows ORDER BY id;
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
-- @check
-- @drain
SELECT * FROM knows ORDER BY id;
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
-- @check
