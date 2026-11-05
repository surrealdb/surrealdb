-- The table type of an edge table changes while a migration of one of its fields is
-- pending, and a normal table holding in and out fields is turned into a relation.
-- Changes the engine refuses leave the table as it was; accepted ones keep every
-- stored row reading as it did, and traversals must find the same edges an edge scan
-- finds.
DEFINE TABLE person SCHEMAFULL;
DEFINE TABLE org SCHEMAFULL;
DEFINE TABLE works TYPE RELATION IN person OUT org SCHEMAFULL;
DEFINE FIELD since ON works TYPE string;
CREATE person:1 RETURN NONE;
CREATE org:1 RETURN NONE;
CREATE org:2 RETURN NONE;
RELATE person:1->works:1->org:1 SET since = '1' RETURN NONE;
RELATE person:1->works:2->org:2 SET since = 'x' RETURN NONE;
DEFINE TABLE plain SCHEMAFULL;
DEFINE FIELD in ON plain TYPE record<person>;
DEFINE FIELD out ON plain TYPE record<org>;
DEFINE FIELD since ON plain TYPE string;
CREATE plain:1 SET in = person:1, out = org:1, since = '4' RETURN NONE;
-- @upgrade
ALTER FIELD since ON works TYPE int MIGRATE EXISTING ROWS AS since_int USING <int>$value FALLBACK 0;
-- Rows behind the step: the type may not change, however it is asked.
ALTER TABLE works TYPE NORMAL;
ALTER TABLE works TYPE ANY;
DEFINE TABLE OVERWRITE works TYPE NORMAL SCHEMAFULL;
RELATE person:1->works:3->org:2 SET since = 3 RETURN NONE;
person:1->works.since;
SELECT * FROM works ORDER BY id;
-- @check
-- A normal table holding in and out fields, with no step behind it, becomes a relation.
ALTER TABLE plain TYPE RELATION;
person:1->plain.since;
SELECT * FROM plain ORDER BY id;
-- @drain
ALTER TABLE works TYPE NORMAL;
SELECT * FROM works ORDER BY id;
person:1->works.since;
RELATE person:1->works:4->org:1 SET since = 4 RETURN NONE;
ALTER TABLE works TYPE RELATION;
person:1->works.since;
SELECT VALUE id FROM works WHERE since > 0 ORDER BY id;
-- @check
DEFINE TABLE OVERWRITE plain TYPE RELATION IN person OUT org SCHEMAFULL;
person:1->plain.since;
SELECT * FROM plain ORDER BY id;
RELATE person:1->plain:2->org:2 SET since = '6' RETURN NONE;
person:1->plain.since;
-- @check
