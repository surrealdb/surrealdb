-- Control for a relation table with edges removed and defined again under the same name, with no
-- field migration anywhere: once the table is removed, its edges are traversed from no node; an
-- edge deleted from a table that stays is traversed from no node at once; and an edge written under
-- an id the removed table used is traversed to its new ends only.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE TABLE knows TYPE RELATION IN person OUT person ENFORCED SCHEMAFULL;
DEFINE FIELD since ON knows TYPE string;
DEFINE TABLE kn TYPE RELATION IN person OUT person ENFORCED SCHEMAFULL;
CREATE person:1 SET name = 'a' RETURN NONE;
CREATE person:2 SET name = 'b' RETURN NONE;
CREATE person:3 SET name = 'c' RETURN NONE;
RELATE person:1->knows:e1->person:2 SET since = '2020' RETURN NONE;
RELATE person:2->knows:e2->person:3 SET since = '2021' RETURN NONE;
RELATE person:1->kn:f1->person:3 RETURN NONE;
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
SELECT VALUE ->kn->person.id FROM person ORDER BY id;
-- @upgrade
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
-- @check
DELETE kn:f1 RETURN NONE;
SELECT VALUE ->kn->person.id FROM person ORDER BY id;
SELECT VALUE <-kn<-person.id FROM person ORDER BY id;
REMOVE TABLE knows;
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
SELECT VALUE <-knows<-person.id FROM person ORDER BY id;
DEFINE TABLE knows TYPE RELATION IN person OUT person ENFORCED SCHEMAFULL;
DEFINE FIELD since ON knows TYPE string;
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
RELATE person:1->knows:e1->person:3 SET since = 'new' RETURN NONE;
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
SELECT VALUE <-knows<-person.id FROM person ORDER BY id;
SELECT * FROM knows ORDER BY id;
-- @drain
SELECT VALUE ->knows->person.id FROM person ORDER BY id;
-- @check
